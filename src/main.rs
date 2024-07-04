use std::fs;
use std::time::{Duration, Instant};
use csv::{ReaderBuilder, Trim};
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[tokio::main(flavor = "multi_thread", worker_threads = 20)]
async fn main() -> anyhow::Result<()> {

    let (sender, recver) = channel(1024);
    let mut handles = Vec::new();

    for _ in 0..16 {
        let sender_clone = sender.clone();

        handles.push(tokio::spawn(async move { senderfn(sender_clone).await }));
    }

    handles.push(tokio::spawn(async move { process(recver).await }));

    for handle in handles {
        handle.await.expect("Error waiting on handle");
    }

    Ok( () )
}

async fn senderfn(sender: Sender<String>) {
    // ~30MB of data
    let csv_data = fs::read_to_string("file.csv").expect("Error reading data");

    loop {
        sender.send(csv_data.clone()).await.expect("Error sending");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}


async fn process(mut recver: Receiver<String>) {
    while let Some(csv) = recver.recv().await {
        let start = Instant::now();
        let mut count = 0;

        let mut reader = ReaderBuilder::new().trim(Trim::All).has_headers(true).from_reader(csv.as_bytes());

        for res in reader.into_records() {
            if let Err(e) = res {
                panic!("Error reading record: {e}")
            }

            count += 1;
        }

        println!("Parsed {} records in {:0.3}s", count, Instant::now().duration_since(start).as_secs_f64());
    }
}
