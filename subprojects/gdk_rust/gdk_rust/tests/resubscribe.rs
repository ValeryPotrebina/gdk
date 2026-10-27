//! Script re-subscription after the syncer rebuilds its Electrum client.

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use gdk_common::log::{info, warn};
use gdk_common::model::TransactionType;
use gdk_common::State;
use gdk_test::{utils, RpcNodeExt, TestSession};
use serde_json::Value;

const SUBSCRIBE_METHOD: &str = "blockchain.scripthash.subscribe";

/// A batched pass has up to 20 subscribe requests in flight, a one-by-one pass at most 1.
const BATCHED_IN_FLIGHT_THRESHOLD: usize = 2;

/// After `disconnect` and `connect` (new syncer thread, new client) a new transaction is
/// notified once and the earlier one is not notified again.
#[test]
fn tx_notification_after_thread_restart() {
    let mut t = TestSession::new(|_| {});
    assert_eq!(t.session.filter_events("transaction").len(), 0);

    let first_txid = t.fund(10_000);
    assert_eq!(t.session.filter_events("transaction").len(), 1);

    t.session.disconnect().unwrap();
    assert_eq!(
        t.session.filter_events("network").last(),
        Some(&utils::ntf_network(State::Disconnected, State::Disconnected))
    );

    let net_params = serde_json::to_value(&t.network).unwrap();
    t.session.connect(&net_params).unwrap();
    assert_eq!(
        t.session.filter_events("network").last(),
        Some(&utils::ntf_network(State::Connected, State::Connected))
    );

    let second_txid = t.fund(20_000);

    // One more sync interval, so a duplicate notification would show up.
    thread::sleep(Duration::from_secs(3));

    let events = t.session.filter_events("transaction");
    let notified: Vec<&str> =
        events.iter().map(|e| e["transaction"]["txhash"].as_str().unwrap()).collect();
    assert_eq!(notified, vec![first_txid.as_str(), second_txid.as_str()], "{events:?}");
    assert_eq!(t.get_tx_list(0).len(), 2);

    t.stop();
}

/// A forwarder between gdk and electrs drops every connection: gdk must report
/// `disconnected`, then `connected`, and re-subscribe the scripts in batches.
#[test]
fn batch_resubscribe_after_connection_loss() {
    let mut forwarder = None;
    let mut t = TestSession::new(|network| {
        let electrs_url = network.electrum_url.take().expect("TestSession sets electrum_url");
        let f = Forwarder::start(&electrs_url);
        network.electrum_url = Some(f.address.clone());
        forwarder = Some(f);
    });
    let forwarder = forwarder.expect("network_conf closure ran");

    // The first sync is batched, so the counter can see batches.
    let first_sync_max = forwarder.max_in_flight_subscribes();
    info!("max subscribe requests in flight during the first sync: {first_sync_max}");
    assert!(
        first_sync_max >= BATCHED_IN_FLIGHT_THRESHOLD,
        "forwarder did not observe the first-sync batches (max in flight {})",
        first_sync_max
    );

    // No batches on a client that keeps its subscriptions.
    forwarder.reset_max_in_flight_subscribes();
    t.fund(10_000);
    let steady_max = forwarder.max_in_flight_subscribes();
    info!("max subscribe requests in flight while funding: {steady_max}");
    assert!(steady_max < BATCHED_IN_FLIGHT_THRESHOLD, "unexpected batch in steady state");

    forwarder.reset_max_in_flight_subscribes();
    forwarder.set_down(true);
    wait_for(Duration::from_secs(60), "network disconnected notification", || {
        t.session.filter_events("network").last()
            == Some(&utils::ntf_network(State::Disconnected, State::Connected))
    });

    // Received while gdk cannot reach the server.
    let amount = 20_000;
    let ap = t.get_receive_address(0);
    let txid = t.node.client.sendtoaddress(&ap.address, amount, None).unwrap();
    t.electrs.trigger().unwrap();

    forwarder.set_down(false);
    wait_for(Duration::from_secs(60), "network connected notification", || {
        t.session.filter_events("network").last()
            == Some(&utils::ntf_network(State::Connected, State::Connected))
    });
    t.wait_tx(vec![0], &txid, Some(amount), Some(TransactionType::Incoming));

    let reconnect_max = forwarder.max_in_flight_subscribes();
    info!("max subscribe requests in flight after the reconnect: {reconnect_max}");
    assert!(
        reconnect_max >= BATCHED_IN_FLIGHT_THRESHOLD,
        "scripts were re-subscribed one by one after the reconnect (max in flight {})",
        reconnect_max
    );

    assert_eq!(
        t.session.filter_events("network"),
        vec![
            utils::ntf_network(State::Connected, State::Connected),
            utils::ntf_network(State::Disconnected, State::Connected),
            utils::ntf_network(State::Connected, State::Connected),
        ]
    );

    t.stop();
    forwarder.stop();
}

fn wait_for<F: FnMut() -> bool>(timeout: Duration, what: &str, mut condition: F) {
    let start = Instant::now();
    while !condition() {
        assert!(start.elapsed() < timeout, "timeout waiting for {}", what);
        thread::sleep(Duration::from_millis(100));
    }
}

/// TCP forwarder between gdk and electrs that counts the subscribe requests in flight;
/// `set_down(true)` drops every connection and closes new ones.
struct Forwarder {
    address: String,
    state: Arc<ForwarderState>,
    acceptor: Option<JoinHandle<()>>,
}

struct ForwarderState {
    upstream: String,
    down: AtomicBool,
    stopping: AtomicBool,
    sockets: Mutex<Vec<TcpStream>>,
    max_in_flight_subscribes: AtomicUsize,
}

#[derive(Clone, Copy)]
enum Direction {
    ToServer,
    ToClient,
}

impl Forwarder {
    fn start(upstream: &str) -> Self {
        // electrsd binds electrs on 0.0.0.0; connect to loopback explicitly.
        let upstream = upstream.replacen("0.0.0.0:", "127.0.0.1:", 1);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        info!("forwarder listening on {address}, upstream {upstream}");

        let state = Arc::new(ForwarderState {
            upstream,
            down: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            sockets: Mutex::new(vec![]),
            max_in_flight_subscribes: AtomicUsize::new(0),
        });

        let acceptor_state = state.clone();
        let acceptor = thread::spawn(move || {
            for incoming in listener.incoming() {
                if acceptor_state.stopping.load(Ordering::SeqCst) {
                    break;
                }
                let client = match incoming {
                    Ok(client) => client,
                    Err(e) => {
                        warn!("forwarder accept failed: {e:?}");
                        continue;
                    }
                };
                if acceptor_state.down.load(Ordering::SeqCst) {
                    let _ = client.shutdown(Shutdown::Both);
                    continue;
                }
                let server = match TcpStream::connect(&acceptor_state.upstream) {
                    Ok(server) => server,
                    Err(e) => {
                        warn!("forwarder cannot reach upstream: {e:?}");
                        let _ = client.shutdown(Shutdown::Both);
                        continue;
                    }
                };
                acceptor_state
                    .sockets
                    .lock()
                    .unwrap()
                    .extend([client.try_clone().unwrap(), server.try_clone().unwrap()]);

                let in_flight = Arc::new(Mutex::new(HashSet::new()));
                let (c2s_state, c2s_in_flight) = (acceptor_state.clone(), in_flight.clone());
                let (client_r, server_w) =
                    (client.try_clone().unwrap(), server.try_clone().unwrap());
                thread::spawn(move || {
                    pump(client_r, server_w, Direction::ToServer, &c2s_in_flight, &c2s_state)
                });
                let (s2c_state, s2c_in_flight) = (acceptor_state.clone(), in_flight);
                thread::spawn(move || {
                    pump(server, client, Direction::ToClient, &s2c_in_flight, &s2c_state)
                });
            }
        });

        Forwarder {
            address,
            state,
            acceptor: Some(acceptor),
        }
    }

    fn set_down(&self, down: bool) {
        self.state.down.store(down, Ordering::SeqCst);
        if down {
            for socket in self.state.sockets.lock().unwrap().drain(..) {
                let _ = socket.shutdown(Shutdown::Both);
            }
        }
    }

    fn max_in_flight_subscribes(&self) -> usize {
        self.state.max_in_flight_subscribes.load(Ordering::SeqCst)
    }

    fn reset_max_in_flight_subscribes(&self) {
        self.state.max_in_flight_subscribes.store(0, Ordering::SeqCst);
    }

    fn stop(mut self) {
        self.state.stopping.store(true, Ordering::SeqCst);
        self.set_down(true);
        // Wake the acceptor so it can observe `stopping`.
        let _ = TcpStream::connect(&self.address);
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
    }
}

fn pump(
    mut from: TcpStream,
    mut to: TcpStream,
    direction: Direction,
    in_flight: &Mutex<HashSet<u64>>,
    state: &ForwarderState,
) {
    let mut buf = [0u8; 64 * 1024];
    let mut pending = Vec::new();
    loop {
        let n = match from.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let chunk = &buf[..n];
        // Count before forwarding, so an answer is never seen before its request.
        pending.extend_from_slice(chunk);
        while let Some(end) = pending.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = pending.drain(..=end).collect();
            track(&line, direction, in_flight, state);
        }
        if to.write_all(chunk).is_err() {
            break;
        }
    }
    let _ = from.shutdown(Shutdown::Both);
    let _ = to.shutdown(Shutdown::Both);
}

fn track(
    line: &[u8],
    direction: Direction,
    in_flight: &Mutex<HashSet<u64>>,
    state: &ForwarderState,
) {
    let value: Value = match serde_json::from_slice(line) {
        Ok(value) => value,
        Err(_) => return,
    };
    let messages = match value {
        Value::Array(items) => items,
        other => vec![other],
    };
    for message in messages {
        // Server-initiated notifications carry no id and are not answers.
        let id = match message.get("id").and_then(Value::as_u64) {
            Some(id) => id,
            None => continue,
        };
        match direction {
            Direction::ToServer => {
                if message.get("method").and_then(Value::as_str) == Some(SUBSCRIBE_METHOD) {
                    let mut set = in_flight.lock().unwrap();
                    set.insert(id);
                    state.max_in_flight_subscribes.fetch_max(set.len(), Ordering::SeqCst);
                }
            }
            Direction::ToClient => {
                in_flight.lock().unwrap().remove(&id);
            }
        }
    }
}
