use std::{io::ErrorKind, net::TcpStream, time::Duration};

use serde_json::{Value, json};
use tungstenite::{
    Error, Message, WebSocket,
    client::IntoClientRequest,
    connect,
    http::{HeaderName, HeaderValue},
    stream::MaybeTlsStream,
};

// A valid UUID v7 used as the correlation id of every test message
const CORRELATION_ID: &str = "0192f3c4-7a5b-7c3e-9d1f-2b4a6c8e0f13";

// A valid UUID used as the request id of every publish
const REQUEST_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

// The topic every test message goes to, it consumes and allows subscriber publishing
const TEST_TOPIC: &str = "gateway-test";

// The topic of the ACK scenario, it needs an ACK from the owner before the offset is committed
const ACK_TOPIC: &str = "gateway-test-ack";

// A second valid UUID v7, for the second ACK message
const CORRELATION_ID_2: &str = "0192f3c4-7a5b-7c3e-9d1f-2b4a6c8e0f14";

// Size of the large message that must go through, under the 10 MB limit (D11)
const BIG_BYTES: usize = 9 * 1024 * 1024;

// Size of the message that must be refused, over the 10 MB limit
const TOO_BIG_BYTES: usize = 11 * 1024 * 1024;

// How long to wait for a large message to travel through Kafka and back
const BIG_WAIT: Duration = Duration::from_secs(30);

// How long to wait for a frame that is expected to arrive
const EXPECTED: Duration = Duration::from_secs(10);

// How long to wait to be sure a frame does not arrive
const NOT_EXPECTED: Duration = Duration::from_secs(3);

// What the scenario needs to know about the running gateway
pub struct Args {
    // Host and port of the gateway, for example localhost:5000
    pub base: String,

    // Client credentials from the env file
    pub client_id: String,
    pub client_secret: String,

    // The API key used for the test connections
    pub api_key: String,
    pub api_secret: String,
}

// The result of one step, the error is the reason it failed
type Step = Result<(), String>;

// Counts the steps and prints each result
#[derive(Default)]
struct Report {
    passed: usize,
    failed: usize,
}

impl Report {
    // Prints one step and counts it
    fn step(&mut self, name: &str, result: Step) {
        match result {
            Ok(()) => {
                println!("PASS  {name}");
                self.passed += 1;
            }
            Err(reason) => {
                println!("FAIL  {name}: {reason}");
                self.failed += 1;
            }
        }
    }
}

// A blocking WebSocket connection to the gateway
struct Client {
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
}

impl Client {
    // Opens a connection to ws-subscribe with the given API secret and an optional subscriber id
    fn open(args: &Args, api_secret: &str, subscriber: Option<&str>) -> Result<Self, Error> {
        Self::open_at(args, "ws-subscribe", api_secret, subscriber)
    }

    // Opens a connection to the given WebSocket route
    fn open_at(
        args: &Args,
        route: &str,
        api_secret: &str,
        subscriber: Option<&str>,
    ) -> Result<Self, Error> {
        let url = format!("ws://{}/api/v1/{route}", args.base);
        let mut request = url.into_client_request()?;

        let mut headers: Vec<(&'static str, &str)> = vec![
            ("x-client-id", args.client_id.as_str()),
            ("x-client-secret", args.client_secret.as_str()),
            ("x-api-key", args.api_key.as_str()),
            ("x-api-secret", api_secret),
        ];

        if let Some(subscriber) = subscriber {
            headers.push(("x-subscriber-id", subscriber));
        }

        for (name, value) in headers {
            // A value that is not valid header text is sent empty and the gateway refuses it
            request.headers_mut().insert(
                HeaderName::from_static(name),
                HeaderValue::from_str(value).unwrap_or_else(|_| HeaderValue::from_static("")),
            );
        }

        let (socket, _) = connect(request)?;

        Ok(Self { socket })
    }

    // Opens a connection to ws-subscribe and waits for the ready frame
    fn open_ready(args: &Args, subscriber: &str) -> Result<Self, String> {
        Self::open_ready_at(args, "ws-subscribe", Some(subscriber))
    }

    // Opens a connection to the given route and waits for the ready frame
    fn open_ready_at(args: &Args, route: &str, subscriber: Option<&str>) -> Result<Self, String> {
        let mut client =
            Self::open_at(args, route, &args.api_secret, subscriber).map_err(|error| {
                format!(
                    "{error}, is the gateway running on {}? (cargo dev)",
                    args.base
                )
            })?;

        let frame = client.read_json(EXPECTED)?;
        expect(&frame, "/type", json!("ready"))?;

        Ok(client)
    }

    // Waits for the next text frame and parses it, pings and pongs are skipped
    fn read_json(&mut self, wait: Duration) -> Result<Value, String> {
        self.set_read_timeout(wait);

        loop {
            match self.socket.read() {
                Ok(Message::Text(text)) => {
                    return serde_json::from_str(text.as_str())
                        .map_err(|error| format!("not JSON: {error}"));
                }
                Ok(Message::Close(frame)) => return Err(format!("connection closed: {frame:?}")),
                Ok(_) => {}
                Err(Error::Io(error))
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
                {
                    return Err("timeout".to_string());
                }
                Err(error) => return Err(error.to_string()),
            }
        }
    }

    // Waits for the close frame and returns its code
    fn read_close_code(&mut self, wait: Duration) -> Result<u16, String> {
        self.set_read_timeout(wait);

        loop {
            match self.socket.read() {
                Ok(Message::Close(Some(frame))) => return Ok(u16::from(frame.code)),
                Ok(Message::Close(None)) => return Err("close frame without a code".to_string()),
                Ok(_) => {}
                Err(error) => return Err(error.to_string()),
            }
        }
    }

    // Sends a JSON value as a text frame
    fn send_json(&mut self, value: Value) -> Step {
        self.send_text(&value.to_string())
    }

    // Sends a text frame
    fn send_text(&mut self, text: &str) -> Step {
        self.socket
            .send(Message::text(text))
            .map_err(|error| error.to_string())
    }

    // Sets how long the next reads wait
    fn set_read_timeout(&mut self, wait: Duration) {
        if let MaybeTlsStream::Plain(stream) = self.socket.get_mut() {
            let _ = stream.set_read_timeout(Some(wait));
        }
    }
}

// Checks that the value has the expected content at the JSON pointer
fn expect(value: &Value, pointer: &str, expected: Value) -> Step {
    match value.pointer(pointer) {
        Some(actual) if *actual == expected => Ok(()),
        _ => Err(format!("expected {pointer} = {expected}, got {value}")),
    }
}

// Runs a step on a client that may have failed to connect
fn with(client: &mut Option<Client>, step: impl FnOnce(&mut Client) -> Step) -> Step {
    match client {
        Some(client) => step(client),
        None => Err("the client is not connected".to_string()),
    }
}

// The publish message every test sends through ws-subscribe
fn publish_message(topic: &str) -> Value {
    json!({
        "type": "publish",
        "requestId": REQUEST_ID,
        "topic": topic,
        "key": "test-ws",
        "value": { "correlationId": CORRELATION_ID, "jobName": "test-ws" }
    })
}

// A wrong API secret gets an error frame and the close code 4001
fn auth_failure(args: &Args) -> Step {
    let mut client = Client::open(args, "wrong-secret", Some("test-ws-auth"))
        .map_err(|error| format!("the upgrade should work, got {error}"))?;

    let frame = client.read_json(EXPECTED)?;
    expect(&frame, "/type", json!("error"))?;
    expect(&frame, "/httpCode", json!(401))?;

    let code = client.read_close_code(EXPECTED)?;
    if code != 4001 {
        return Err(format!("expected close code 4001, got {code}"));
    }

    Ok(())
}

// A missing x-subscriber-id is refused with 400 before the upgrade
fn missing_subscriber_id(args: &Args) -> Step {
    match Client::open(args, &args.api_secret, None) {
        Err(Error::Http(response)) if response.status() == 400 => Ok(()),
        Err(error) => Err(format!("expected an HTTP 400 answer, got {error}")),
        Ok(_) => Err("the upgrade should have been refused".to_string()),
    }
}

// The publish message of the ACK scenario
fn ack_publish(correlation_id: &str) -> Value {
    json!({
        "type": "publish",
        "requestId": REQUEST_ID,
        "topic": ACK_TOPIC,
        "key": "test-ws-ack",
        "value": { "correlationId": correlation_id, "jobName": "test-ws-ack" }
    })
}

// Subscribes a client to a topic and checks the answer
fn subscribe(client: &mut Client, topic: &str) -> Step {
    client.send_json(json!({ "type": "subscribe", "topic": topic }))?;
    let frame = client.read_json(EXPECTED)?;
    expect(&frame, "/type", json!("subscribed"))
}

// Publishes a message and checks the published frame
fn publish_and_confirm(client: &mut Client, message: Value) -> Step {
    client.send_json(message)?;
    let frame = client.read_json(EXPECTED)?;
    expect(&frame, "/type", json!("published"))
}

// Reads one event of the ACK topic and returns its offset
fn read_ack_event(client: &mut Client, correlation_id: &str) -> Result<String, String> {
    let frame = client.read_json(EXPECTED)?;
    expect(&frame, "/type", json!("event"))?;
    expect(&frame, "/topic", json!(ACK_TOPIC))?;
    expect(&frame, "/correlationId", json!(correlation_id))?;

    frame["offset"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| format!("the event has no offset: {frame}"))
}

// Sends processed and checks that the gateway accepted it
fn acknowledge(client: &mut Client, correlation_id: &str) -> Step {
    client.send_json(json!({ "type": "processed", "correlationId": correlation_id }))?;
    let frame = client.read_json(EXPECTED)?;
    expect(&frame, "/type", json!("ack-received"))?;
    expect(&frame, "/correlationId", json!(correlation_id))
}

// The ACK scenario: owner and non-owner, then redelivery after the owner leaves without acknowledging
fn run_ack(args: &Args, report: &mut Report) {
    println!();
    println!("ACK scenario on {ACK_TOPIC}");

    // C connects first, so it has the lowest id and owns the topic
    let c = Client::open_ready(args, "test-ws-c");
    report.step("C connects", c.as_ref().map(|_| ()).map_err(Clone::clone));
    let mut c = c.ok();
    report.step(
        "C subscribes to the ACK topic",
        with(&mut c, |c| subscribe(c, ACK_TOPIC)),
    );

    let d = Client::open_ready(args, "test-ws-d");
    report.step("D connects", d.as_ref().map(|_| ()).map_err(Clone::clone));
    let mut d = d.ok();
    report.step(
        "D subscribes to the ACK topic",
        with(&mut d, |d| subscribe(d, ACK_TOPIC)),
    );

    let p = Client::open_ready(args, "test-ws-p");
    report.step("P connects", p.as_ref().map(|_| ()).map_err(Clone::clone));
    let mut p = p.ok();

    report.step(
        "P publishes message 1 to the ACK topic",
        with(&mut p, |p| {
            publish_and_confirm(p, ack_publish(CORRELATION_ID))
        }),
    );

    report.step(
        "C and D both receive message 1",
        with(&mut c, |c| read_ack_event(c, CORRELATION_ID).map(|_| ()))
            .and_then(|()| with(&mut d, |d| read_ack_event(d, CORRELATION_ID).map(|_| ()))),
    );

    report.step(
        "D is refused when it acknowledges, it does not own the message",
        with(&mut d, |d| {
            d.send_json(json!({ "type": "processed", "correlationId": CORRELATION_ID }))?;
            let frame = d.read_json(EXPECTED)?;
            expect(&frame, "/message", json!("ACK rejected"))?;
            expect(&frame, "/reason", json!("ACK from non-owning connection"))
        }),
    );

    report.step(
        "C acknowledges message 1 and gets ack-received",
        with(&mut c, |c| acknowledge(c, CORRELATION_ID)),
    );

    report.step(
        "P publishes message 2 to the ACK topic",
        with(&mut p, |p| {
            publish_and_confirm(p, ack_publish(CORRELATION_ID_2))
        }),
    );

    // D keeps the offset of its first delivery, redelivery must bring the same one
    let mut first_offset = Err("message 2 was not received by D".to_string());

    report.step(
        "C and D both receive message 2",
        with(&mut c, |c| read_ack_event(c, CORRELATION_ID_2).map(|_| ())).and_then(|()| {
            with(&mut d, |d| {
                first_offset = read_ack_event(d, CORRELATION_ID_2);
                first_offset.clone().map(|_| ())
            })
        }),
    );

    // C leaves without acknowledging, the gateway has to read message 2 again
    drop(c.take());

    report.step(
        "D receives message 2 again at the same offset after C left without acknowledging",
        with(&mut d, |d| {
            let first = first_offset.clone()?;
            let again = read_ack_event(d, CORRELATION_ID_2)?;
            if again == first {
                Ok(())
            } else {
                Err(format!("expected offset {first}, got {again}"))
            }
        }),
    );

    report.step(
        "D is now the owner and acknowledges message 2",
        with(&mut d, |d| acknowledge(d, CORRELATION_ID_2)),
    );
}

// The ws-publish scenario: a job published through ws-publish reaches a subscriber through Kafka
fn run_ws_publish(args: &Args, report: &mut Report) {
    println!();
    println!("ws-publish scenario on {TEST_TOPIC}");

    // E listens on the test topic
    let e = Client::open_ready(args, "test-ws-e");
    report.step(
        "E connects to ws-subscribe",
        e.as_ref().map(|_| ()).map_err(Clone::clone),
    );
    let mut e = e.ok();
    report.step(
        "E subscribes to the test topic",
        with(&mut e, |e| subscribe(e, TEST_TOPIC)),
    );

    // The job client connects to ws-publish, which has no subscriber id
    let f = Client::open_ready_at(args, "ws-publish", None);
    report.step(
        "F connects to ws-publish and gets the ready frame",
        f.as_ref().map(|_| ()).map_err(Clone::clone),
    );
    let mut f = f.ok();

    // The correlation id is generated by the gateway, the ack carries it
    let mut correlation_id = Err("no ack received".to_string());

    report.step(
        "F publishes a job and gets an ack with a correlation id",
        with(&mut f, |f| {
            f.send_json(json!({ "topic": TEST_TOPIC, "jobName": "test-ws-job" }))?;
            let frame = f.read_json(EXPECTED)?;
            expect(&frame, "/type", json!("ack"))?;
            expect(&frame, "/topic", json!(TEST_TOPIC))?;

            correlation_id = frame["correlationId"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("the ack has no correlationId: {frame}"));

            correlation_id.clone().map(|_| ())
        }),
    );

    report.step(
        "E receives the job with the same correlation id and the job fields",
        with(&mut e, |e| {
            let expected = correlation_id.clone()?;
            let frame = e.read_json(EXPECTED)?;
            expect(&frame, "/type", json!("event"))?;
            expect(&frame, "/topic", json!(TEST_TOPIC))?;
            expect(&frame, "/correlationId", json!(expected))?;
            expect(&frame, "/jobName", json!("test-ws-job"))?;

            if frame.pointer("/apiUserId").is_none() || frame.pointer("/timestamp").is_none() {
                return Err(format!("apiUserId or timestamp is missing: {frame}"));
            }

            Ok(())
        }),
    );

    report.step(
        "F publishing to a topic that is not enabled is refused",
        with(&mut f, |f| {
            f.send_json(json!({ "topic": "no-such-topic", "jobName": "test-ws-job" }))?;
            let frame = f.read_json(EXPECTED)?;
            expect(&frame, "/message", json!("Unauthorized topic"))?;
            expect(&frame, "/reason", json!("topic-not-allowed"))
        }),
    );

    report.step(
        "F gets Malformed JSON and Invalid publish message error frames",
        with(&mut f, |f| {
            f.send_text("not json")?;
            let frame = f.read_json(EXPECTED)?;
            expect(&frame, "/message", json!("Malformed JSON"))?;

            f.send_json(json!({ "topic": TEST_TOPIC }))?;
            let frame = f.read_json(EXPECTED)?;
            expect(&frame, "/message", json!("Invalid publish message"))
        }),
    );
}

// Connects a subscriber and waits for the gateway to close the connection, used to test the graceful shutdown
pub fn hold(args: &Args) -> bool {
    println!("Gateway: ws://{}/api/v1/ws-subscribe", args.base);

    let mut client = match Client::open_ready(args, "test-ws-hold") {
        Ok(client) => client,
        Err(reason) => {
            println!("FAIL  connect: {reason}");
            return false;
        }
    };

    if let Err(reason) = subscribe(&mut client, ACK_TOPIC) {
        println!("FAIL  subscribe: {reason}");
        return false;
    }

    println!("Connected and subscribed to {ACK_TOPIC}.");
    println!(
        "Now press Ctrl+C in the terminal that runs the gateway, waiting up to 60 s for the close frame..."
    );

    match client.read_close_code(Duration::from_secs(60)) {
        Ok(1001) => {
            println!("PASS  the gateway closed the connection with code 1001");
            true
        }
        Ok(code) => {
            println!("FAIL  expected close code 1001, got {code}");
            false
        }
        Err(reason) => {
            println!("FAIL  no close frame: {reason}");
            false
        }
    }
}
// A publish message whose value carries a blob of the given size
fn big_publish(blob_bytes: usize) -> Value {
    json!({
        "type": "publish",
        "requestId": REQUEST_ID,
        "topic": TEST_TOPIC,
        "key": "test-ws-big",
        "value": {
            "correlationId": CORRELATION_ID,
            "jobName": "test-ws-big",
            "blob": "x".repeat(blob_bytes)
        }
    })
}

// The message size scenario (D11): 9 MB goes through Kafka and back, 11 MB is refused
fn run_big(args: &Args, report: &mut Report) {
    println!();
    println!("message size scenario on {TEST_TOPIC}");

    let g = Client::open_ready(args, "test-ws-g");
    report.step("G connects", g.as_ref().map(|_| ()).map_err(Clone::clone));
    let mut g = g.ok();
    report.step(
        "G subscribes to the test topic",
        with(&mut g, |g| subscribe(g, TEST_TOPIC)),
    );

    let h = Client::open_ready(args, "test-ws-h");
    report.step("H connects", h.as_ref().map(|_| ()).map_err(Clone::clone));
    let mut h = h.ok();

    report.step(
        "H publishes a 9 MB message and Kafka accepts it",
        with(&mut h, |h| {
            h.send_json(big_publish(BIG_BYTES))?;
            let frame = h.read_json(BIG_WAIT)?;
            expect(&frame, "/type", json!("published"))
        }),
    );

    report.step(
        "G receives the 9 MB message through Kafka and the bridge",
        with(&mut g, |g| {
            let frame = g.read_json(BIG_WAIT)?;
            expect(&frame, "/type", json!("event"))?;
            expect(&frame, "/correlationId", json!(CORRELATION_ID))?;

            let length = frame["blob"].as_str().map_or(0, str::len);
            if length == BIG_BYTES {
                Ok(())
            } else {
                Err(format!(
                    "expected a blob of {BIG_BYTES} bytes, got {length}"
                ))
            }
        }),
    );

    let i = Client::open_ready(args, "test-ws-i");
    report.step("I connects", i.as_ref().map(|_| ()).map_err(Clone::clone));
    let mut i = i.ok();

    report.step(
        "an 11 MB message makes the gateway end the connection",
        with(&mut i, |i| {
            // The send may fail half way because the gateway already closed the connection, that is the expected outcome too
            let _ = i.send_json(big_publish(TOO_BIG_BYTES));

            match i.read_json(EXPECTED) {
                Err(reason) if reason != "timeout" => Ok(()),
                Err(reason) => Err(format!("the connection stayed open: {reason}")),
                Ok(frame) => Err(format!("unexpected frame: {frame}")),
            }
        }),
    );
}

// Runs the whole scenario against the running gateway, returns true when every step passed
pub fn run(args: &Args) -> bool {
    let mut report = Report::default();

    println!("Gateway: ws://{}/api/v1/ws-subscribe", args.base);
    println!();

    report.step(
        "a wrong API secret gets an error frame, then close code 4001",
        auth_failure(args),
    );
    report.step(
        "a missing x-subscriber-id is refused with 400 before the upgrade",
        missing_subscriber_id(args),
    );

    // Subscriber A listens, subscriber B publishes
    let a = Client::open_ready(args, "test-ws-a");
    report.step(
        "subscriber A connects and gets the ready frame",
        a.as_ref().map(|_| ()).map_err(Clone::clone),
    );
    let mut a = a.ok();

    report.step(
        "A subscribes to the test topic",
        with(&mut a, |a| {
            a.send_json(json!({ "type": "subscribe", "topic": TEST_TOPIC }))?;
            let frame = a.read_json(EXPECTED)?;
            expect(&frame, "/type", json!("subscribed"))?;
            expect(&frame, "/topic", json!(TEST_TOPIC))
        }),
    );

    report.step(
        "text that is not JSON gets a Malformed JSON error frame",
        with(&mut a, |a| {
            a.send_text("not json")?;
            let frame = a.read_json(EXPECTED)?;
            expect(&frame, "/type", json!("error"))?;
            expect(&frame, "/message", json!("Malformed JSON"))
        }),
    );

    report.step(
        "an invalid message gets an error frame with issues",
        with(&mut a, |a| {
            a.send_json(json!({ "type": "subscribe", "topic": "" }))?;
            let frame = a.read_json(EXPECTED)?;
            expect(&frame, "/message", json!("Invalid message format"))?;
            expect(
                &frame,
                "/issues/0/message",
                json!("\"topic\" must not be empty"),
            )
        }),
    );

    let b = Client::open_ready(args, "test-ws-b");
    report.step(
        "subscriber B connects and gets the ready frame",
        b.as_ref().map(|_| ()).map_err(Clone::clone),
    );
    let mut b = b.ok();

    report.step(
        "B publishes to the test topic and gets a published frame",
        with(&mut b, |b| {
            b.send_json(publish_message(TEST_TOPIC))?;
            let frame = b.read_json(EXPECTED)?;
            expect(&frame, "/type", json!("published"))?;
            expect(&frame, "/requestId", json!(REQUEST_ID))?;
            expect(&frame, "/topic", json!(TEST_TOPIC))
        }),
    );

    report.step(
        "A receives the event that B published, through Kafka and the bridge",
        with(&mut a, |a| {
            let frame = a.read_json(EXPECTED)?;
            expect(&frame, "/type", json!("event"))?;
            expect(&frame, "/topic", json!(TEST_TOPIC))?;
            expect(&frame, "/correlationId", json!(CORRELATION_ID))?;
            expect(&frame, "/jobName", json!("test-ws"))
        }),
    );

    report.step(
        "B publishing to a topic that is not allowed is refused",
        with(&mut b, |b| {
            b.send_json(publish_message("no-such-topic"))?;
            let frame = b.read_json(EXPECTED)?;
            expect(&frame, "/message", json!("Unauthorized topic"))?;
            expect(&frame, "/reason", json!("topic-not-allowed"))?;
            expect(&frame, "/requestId", json!(REQUEST_ID))
        }),
    );

    report.step(
        "A unsubscribes from the test topic",
        with(&mut a, |a| {
            a.send_json(json!({ "type": "unsubscribe", "topic": TEST_TOPIC }))?;
            let frame = a.read_json(EXPECTED)?;
            expect(&frame, "/type", json!("unsubscribed"))
        }),
    );

    // B publishes again, A has to stay silent
    let republished = with(&mut b, |b| {
        b.send_json(publish_message(TEST_TOPIC))?;
        let frame = b.read_json(EXPECTED)?;
        expect(&frame, "/type", json!("published"))
    });
    report.step("B publishes a second message", republished);

    report.step(
        "A receives nothing after unsubscribing",
        with(&mut a, |a| match a.read_json(NOT_EXPECTED) {
            Err(reason) if reason == "timeout" => Ok(()),
            Err(reason) => Err(reason),
            Ok(frame) => Err(format!("unexpected frame: {frame}")),
        }),
    );
    run_ack(args, &mut report);
    run_ws_publish(args, &mut report);
    run_big(args, &mut report);
    println!();
    println!("{} passed, {} failed", report.passed, report.failed);

    report.failed == 0
}
