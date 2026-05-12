use borsh::{BorshDeserialize, BorshSerialize};
use lapin::{Connection, ConnectionProperties, options::*, types::FieldTable};
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize, Serialize, Deserialize)]
pub struct UserCreatedEventMessage {
    pub user_id: String,
    pub user_name: String
}

#[tokio::main]
async fn main() {
    let conn = Connection::connect(
        "amqp://guest:guest@localhost:5672",
        ConnectionProperties::default(),
    )
    .await
    .expect("Failed to connect to RabbitMQ");

    let channel = conn
        .create_channel()
        .await
        .expect("Failed to create channel");

    let queue = channel
        .queue_declare(
            "user_created",
            QueueDeclareOptions {
                durable: true,
                auto_delete: false,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("Failed to declare queue");

    let mut consumer = channel
        .basic_consume(
            "user_created",
            "subscriber_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to consume messages");

    println!("Waiting for messages on queue: {}", queue.name());

    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                match serde_json::from_slice::<UserCreatedEventMessage>(&delivery.data) {
                    Ok(message) => {
                        println!(
                            "In Anderson's (2406355893) Computer [129500004y]. Message received: {:?}",
                            message
                        );
                        // Simulate slow processing with 1 second delay
                        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                    }
                    Err(e) => {
                        eprintln!("Failed to deserialize message: {}", e);
                    }
                }
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
            Err(e) => {
                eprintln!("Error consuming message: {}", e);
            }
        }
    }
}