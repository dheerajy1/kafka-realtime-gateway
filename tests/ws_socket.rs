// WebSocket tests over a real socket: the real router listens on 127.0.0.1 with a free port and a real client connects to it
// The Kafka side is the in-memory fakes, so no Kafka and no database are needed
mod common;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use common::{
    KafkaFakes, TEST_CLIENT_ID, TEST_CLIENT_SECRET, app_with, fakes::FakeStore, state_with_kafka,
};
use futures_util::{SinkExt, StreamExt};
use kafka_api_gateway::{kafka::inbound::is_uuid_v7, state::AppState};
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        Error, Message,
        client::IntoClientRequest,
        http::{HeaderName, HeaderValue},
    },
};

// Credentials of the API key the fake store knows
const API_KEY: &str = "key-1";
const API_SECRET: &str = "s3cret";

// How long a test waits for a frame before it fails
const WAIT: Duration = Duration::from_secs(5);

// The client side of one connection
type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

// A fake store with one API key and the topic orders
fn store() -> Arc<FakeStore> {
    Arc::new(
        FakeStore::new()
            .with_api_key(API_KEY, API_SECRET)
            .with_topic("orders", true, false, true, true),
    )
}

// The headers of a connection that passes every check
fn good_headers() -> [(&'static str, &'static str); 4] {
    [
        ("x-client-id", TEST_CLIENT_ID),
        ("x-client-secret", TEST_CLIENT_SECRET),
        ("x-api-key", API_KEY),
        ("x-api-secret", API_SECRET),
    ]
}

// Starts the real router on a free local port, the server task ends when the test ends
async fn serve(state: AppState) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = app_with(state);

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    address
}

// Opens a connection with the given headers
async fn try_connect(
    address: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
) -> Result<Client, Error> {
    let mut request = format!("ws://{address}{path}").into_client_request()?;

    for (name, value) in headers {
        request.headers_mut().insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }

    let (socket, _) = connect_async(request).await?;

    Ok(socket)
}

// Opens a connection that must succeed
async fn connect(address: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Client {
    try_connect(address, path, headers).await.unwrap()
}

// Reads the next message, a test fails when nothing comes in time
async fn next_message(socket: &mut Client) -> Message {
    timeout(WAIT, socket.next())
        .await
        .expect("no message in time")
        .expect("the connection ended")
        .expect("the connection failed")
}

// Reads the next text frame as JSON, pings are skipped
async fn next_frame(socket: &mut Client) -> Value {
    loop {
        match next_message(socket).await {
            Message::Text(text) => return serde_json::from_str(text.as_str()).unwrap(),
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("expected a text frame, got {other:?}"),
        }
    }
}

// Reads the next message, which must be a close frame, and returns its code
async fn next_close_code(socket: &mut Client) -> u16 {
    match next_message(socket).await {
        Message::Close(Some(frame)) => u16::from(frame.code),
        other => panic!("expected a close frame, got {other:?}"),
    }
}

// Sends a JSON message
async fn send(socket: &mut Client, message: Value) {
    socket
        .send(Message::Text(message.to_string().into()))
        .await
        .unwrap();
}

// A publish connection answers ready, publishes a message and answers with an ack
#[tokio::test]
async fn publish_over_a_real_socket_reaches_the_publisher() {
    let fakes = KafkaFakes::new();
    let address = serve(state_with_kafka(store(), &fakes)).await;

    let mut socket = connect(address, "/api/v1/ws-publish", &good_headers()).await;

    assert_eq!(next_frame(&mut socket).await["type"], "ready");

    send(
        &mut socket,
        json!({ "type": "publish", "topic": "orders", "jobName": "sync" }),
    )
    .await;

    let ack = next_frame(&mut socket).await;

    assert_eq!(ack["type"], "ack");
    assert_eq!(ack["topic"], "orders");
    assert!(is_uuid_v7(ack["correlationId"].as_str().unwrap()));

    let sent = fakes.publisher.sent();

    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].topic, "orders");
    assert_eq!(sent[0].key.as_deref(), Some(API_KEY));
}

// A message that is not JSON gets an error frame and the connection stays open
#[tokio::test]
async fn malformed_message_gets_an_error_and_the_connection_stays_open() {
    let fakes = KafkaFakes::new();
    let address = serve(state_with_kafka(store(), &fakes)).await;

    let mut socket = connect(address, "/api/v1/ws-publish", &good_headers()).await;
    next_frame(&mut socket).await;

    socket.send(Message::Text("not json".into())).await.unwrap();

    let error = next_frame(&mut socket).await;

    assert_eq!(error["type"], "error");
    assert_eq!(error["message"], "Malformed JSON");

    send(
        &mut socket,
        json!({ "type": "publish", "topic": "orders", "jobName": "sync" }),
    )
    .await;

    assert_eq!(next_frame(&mut socket).await["type"], "ack");
}

// A wrong API secret gets an error frame and the close code 4001
#[tokio::test]
async fn wrong_api_secret_gets_an_error_frame_and_close_4001() {
    let fakes = KafkaFakes::new();
    let address = serve(state_with_kafka(store(), &fakes)).await;

    let headers = [
        ("x-client-id", TEST_CLIENT_ID),
        ("x-client-secret", TEST_CLIENT_SECRET),
        ("x-api-key", API_KEY),
        ("x-api-secret", "wrong"),
    ];
    let mut socket = connect(address, "/api/v1/ws-publish", &headers).await;

    let error = next_frame(&mut socket).await;

    assert_eq!(error["type"], "error");
    assert_eq!(error["success"], false);
    assert_eq!(error["statusCode"], 401);
    assert_eq!(next_close_code(&mut socket).await, 4001);
    assert!(fakes.publisher.sent().is_empty());
}

// A subscribe connection without the subscriber id is refused before the upgrade
#[tokio::test]
async fn subscribe_without_subscriber_id_is_refused_with_400() {
    let fakes = KafkaFakes::new();
    let address = serve(state_with_kafka(store(), &fakes)).await;

    let result = try_connect(address, "/api/v1/ws-subscribe", &good_headers()).await;

    match result {
        Err(Error::Http(response)) => assert_eq!(response.status().as_u16(), 400),
        Err(other) => panic!("expected an HTTP 400, got the error {other}"),
        Ok(_) => panic!("expected an HTTP 400, got an open connection"),
    }
}

// One delivered frame reaches every subscriber of the topic through its own real socket
#[tokio::test]
async fn delivered_frame_reaches_every_subscriber_of_the_topic() {
    let fakes = KafkaFakes::new();
    let address = serve(state_with_kafka(store(), &fakes)).await;

    let mut sockets = Vec::new();

    for name in ["billing", "audit"] {
        let [client_id, client_secret, api_key, api_secret] = good_headers();
        let headers = [
            client_id,
            client_secret,
            api_key,
            api_secret,
            ("x-subscriber-id", name),
        ];

        let mut socket = connect(address, "/api/v1/ws-subscribe", &headers).await;
        assert_eq!(next_frame(&mut socket).await["type"], "ready");

        send(
            &mut socket,
            json!({ "type": "subscribe", "topic": "orders" }),
        )
        .await;
        assert_eq!(next_frame(&mut socket).await["type"], "subscribed");

        sockets.push(socket);
    }

    let report = fakes
        .subscribers
        .deliver("orders", json!({ "type": "event", "n": 1 }).to_string());

    assert_eq!(report.delivered, 2);

    for socket in &mut sockets {
        let frame = next_frame(socket).await;

        assert_eq!(frame, json!({ "type": "event", "n": 1 }));
    }
}

// A shutdown closes an open connection with the going away code
#[tokio::test]
async fn shutdown_closes_the_connection_with_1001() {
    let fakes = KafkaFakes::new();
    let address = serve(state_with_kafka(store(), &fakes)).await;

    let mut socket = connect(address, "/api/v1/ws-publish", &good_headers()).await;
    next_frame(&mut socket).await;

    fakes.stop.send(true).unwrap();

    assert_eq!(next_close_code(&mut socket).await, 1001);
}
