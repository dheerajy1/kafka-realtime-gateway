use std::time::Duration;

use kafkit_client::{
    AdminConfig, AutoOffsetReset, CommitOffset, ConsumerConfig, KafkaAdmin, KafkaConsumer,
    KafkaProducer, NewTopic, ProduceRecord, ProducerConfig, TopicPartition,
};

const BROKERS: &str = "localhost:9092";
const TOPIC_A: &str = "spike-a";
const TOPIC_B: &str = "spike-b";

// One consumed record: topic, partition, offset, value length.
type Seen = (String, i32, i64, usize);

// Polls for the given seconds and returns every record received.
async fn drain(consumer: &KafkaConsumer, secs: u64) -> kafkit_client::Result<Vec<Seen>> {
    let mut seen = Vec::new();
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    while tokio::time::Instant::now() < end {
        let records = consumer.poll_for(Duration::from_millis(500)).await?;
        for r in records.iter() {
            let len = r.value.as_ref().map_or(0, |v| v.len());
            seen.push((r.topic.clone(), r.partition, r.offset, len));
        }
    }
    Ok(seen)
}

// Sends one text record and prints where it landed.
async fn send(p: &KafkaProducer, topic: &str, text: &str) -> kafkit_client::Result<()> {
    let ack = p
        .send(ProduceRecord::without_partition(topic, text.to_owned()))
        .await?;
    println!(
        "  sent {text} -> {}:{}@{}",
        ack.topic, ack.partition, ack.offset
    );
    Ok(())
}

// Prints a heading and what a drain received.
fn show(title: &str, seen: &[Seen]) {
    println!("{title}");
    if seen.is_empty() {
        println!("  (nothing received)");
    }
    for (topic, partition, offset, len) in seen {
        println!("  got {topic}:{partition}@{offset} ({len} bytes)");
    }
}

#[tokio::main]
async fn main() -> kafkit_client::Result<()> {
    // VERIFY 3: admin call that lists topics, plus topic creation for the spike.
    let admin = KafkaAdmin::connect(AdminConfig::new(BROKERS)).await?;
    let _ = admin
        .create_topics([NewTopic::new(TOPIC_A, 1, 1), NewTopic::new(TOPIC_B, 1, 1)])
        .await;
    let names: Vec<String> = admin
        .list_topics()
        .await?
        .into_iter()
        .map(|t| t.name)
        .collect();
    println!("VERIFY 3 admin.list_topics: {names:?}");

    // VERIFY 6: acks all (-1) plus idempotence, and a raised request size.
    let producer = KafkaProducer::connect(
        ProducerConfig::new(BROKERS)
            .with_acks(-1)
            .with_enable_idempotence(true)
            .with_max_request_size(11 * 1024 * 1024),
    )
    .await?;

    let consumer = KafkaConsumer::connect(
        ConsumerConfig::new(BROKERS, "spike-group").with_auto_offset_reset(AutoOffsetReset::Latest),
    )
    .await?;
    let part_a = TopicPartition::new(TOPIC_A, 0);

    // VERIFY 1: does a second subscribe replace the first?
    consumer.subscribe(vec![TOPIC_A.to_owned()]).await?;
    consumer.subscribe(vec![TOPIC_B.to_owned()]).await?;
    // Wait for the group join so the latest position is fixed before sending.
    drain(&consumer, 10).await?;
    send(&producer, TOPIC_A, "t1-a").await?;
    send(&producer, TOPIC_B, "t1-b").await?;
    show(
        "VERIFY 1 (only b means replace, both means add)",
        &drain(&consumer, 5).await?,
    );

    // VERIFY 2: a smaller list drops the removed topic.
    consumer
        .subscribe(vec![TOPIC_A.to_owned(), TOPIC_B.to_owned()])
        .await?;
    drain(&consumer, 3).await?;
    send(&producer, TOPIC_A, "t2-a").await?;
    send(&producer, TOPIC_B, "t2-b").await?;
    show(
        "VERIFY 2 step 1 (a and b expected)",
        &drain(&consumer, 5).await?,
    );
    consumer.subscribe(vec![TOPIC_A.to_owned()]).await?;
    drain(&consumer, 3).await?;
    send(&producer, TOPIC_A, "t2-a2").await?;
    send(&producer, TOPIC_B, "t2-b2").await?;
    show(
        "VERIFY 2 step 2 (only a expected)",
        &drain(&consumer, 5).await?,
    );

    // VERIFY 4: commit offset + 1 and read it back.
    consumer
        .subscribe(vec![TOPIC_A.to_owned(), TOPIC_B.to_owned()])
        .await?;
    drain(&consumer, 3).await?;
    send(&producer, TOPIC_A, "t4-a").await?;
    let seen = drain(&consumer, 4).await?;
    show("VERIFY 4 received", &seen);
    if let Some((_, _, offset, _)) = seen.iter().rev().find(|s| s.0 == TOPIC_A) {
        consumer
            .commit_offsets(vec![CommitOffset {
                topic: TOPIC_A.to_owned(),
                partition: 0,
                offset: offset + 1,
            }])
            .await?;
        let committed = consumer.committed(vec![part_a.clone()]).await?;
        println!("  committed {offset}+1, broker reports {committed:?}");
    }

    // VERIFY 5: seek back on a while b keeps flowing, then pause and resume a.
    send(&producer, TOPIC_A, "t5-a1").await?;
    send(&producer, TOPIC_A, "t5-a2").await?;
    let seen = drain(&consumer, 4).await?;
    show("VERIFY 5 first delivery", &seen);
    if let Some((_, _, first, _)) = seen.iter().find(|s| s.0 == TOPIC_A) {
        consumer.seek(part_a.clone(), *first).await?;
        send(&producer, TOPIC_B, "t5-b").await?;
        show(
            "VERIFY 5 after seek (a replayed, b flowing)",
            &drain(&consumer, 5).await?,
        );
    }
    consumer.pause(vec![part_a.clone()]).await?;
    send(&producer, TOPIC_A, "t5-pa").await?;
    send(&producer, TOPIC_B, "t5-pb").await?;
    show(
        "VERIFY 5 while a paused (only b expected)",
        &drain(&consumer, 4).await?,
    );
    consumer.resume(vec![part_a.clone()]).await?;
    show(
        "VERIFY 5 after resume (a expected)",
        &drain(&consumer, 4).await?,
    );

    // Size check: a 2 MiB record, sent and consumed with default fetch settings.
    let big = vec![b'x'; 2 * 1024 * 1024];
    match producer
        .send(ProduceRecord::without_partition(TOPIC_A, big))
        .await
    {
        Ok(ack) => println!("2 MiB send ok at offset {}", ack.offset),
        Err(error) => println!("2 MiB send failed: {error}"),
    }
    show("2 MiB consume", &drain(&consumer, 6).await?);

    consumer.shutdown().await?;
    producer.shutdown().await?;
    Ok(())
}
