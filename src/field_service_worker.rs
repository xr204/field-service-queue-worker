mod infrai_queue;

use std::{sync::Arc, time::Duration};

use infrai_queue::{InfraiQueue, QueueError, QueueMessage};
use serde::Deserialize;
use thiserror::Error;
use tokio::{sync::Semaphore, task::JoinSet, time::interval};

const CONCURRENCY: usize = 4;
const STARTS_PER_SECOND: u64 = 2;

#[derive(Debug, Deserialize)]
struct WorkOrder {
    work_order_id: String,
    dispatch_status: DispatchStatus,
    photos: Vec<WorkOrderPhoto>,
    technician_note: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum DispatchStatus {
    Assigned,
    EnRoute,
    OnSite,
    Completed,
    Blocked,
}

#[derive(Debug, Deserialize)]
struct WorkOrderPhoto {
    photo_id: String,
    kind: PhotoKind,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PhotoKind {
    Arrival,
    Completion,
    Equipment,
}

#[derive(Debug, PartialEq)]
enum FollowUp {
    None,
    RequestCompletionPhoto,
    ContactTechnician,
}

#[derive(Debug, Error)]
enum WorkerError {
    #[error(transparent)]
    Queue(#[from] QueueError),
    #[error("worker task could not join: {0}")]
    Join(#[from] tokio::task::JoinError),
}

fn decide_follow_up(order: &WorkOrder) -> FollowUp {
    if order.dispatch_status == DispatchStatus::Blocked {
        return FollowUp::ContactTechnician;
    }
    let has_completion_photo = order
        .photos
        .iter()
        .any(|photo| matches!(photo.kind, PhotoKind::Completion));
    if order.dispatch_status == DispatchStatus::Completed && !has_completion_photo {
        return FollowUp::RequestCompletionPhoto;
    }
    FollowUp::None
}

async fn process(message: &QueueMessage<WorkOrder>) {
    let order = &message.payload;
    let photo_ids: Vec<&str> = order.photos.iter().map(|photo| photo.photo_id.as_str()).collect();
    println!(
        "work_order={} status={:?} photos={:?} note_present={} follow_up={:?}",
        order.work_order_id,
        order.dispatch_status,
        photo_ids,
        order.technician_note.is_some(),
        decide_follow_up(order)
    );
}

#[tokio::main]
async fn main() -> Result<(), WorkerError> {
    let queue = Arc::new(InfraiQueue::from_env()?);
    let permits = Arc::new(Semaphore::new(CONCURRENCY));
    let messages = queue.consume::<WorkOrder>(10, 60).await?;
    let mut starts = interval(Duration::from_millis(1_000 / STARTS_PER_SECOND));
    let mut tasks = JoinSet::new();

    for message in messages {
        starts.tick().await;
        let permit = permits.clone().acquire_owned().await.expect("semaphore open");
        let queue = queue.clone();
        tasks.spawn(async move {
            let _permit = permit;
            process(&message).await;
            queue.ack(&message.message_id).await
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(status: DispatchStatus, photos: Vec<PhotoKind>) -> WorkOrder {
        WorkOrder {
            work_order_id: "wo-1042".to_owned(),
            dispatch_status: status,
            photos: photos
                .into_iter()
                .enumerate()
                .map(|(index, kind)| WorkOrderPhoto {
                    photo_id: format!("photo-{index}"),
                    kind,
                })
                .collect(),
            technician_note: None,
        }
    }

    #[test]
    fn completed_order_without_completion_photo_needs_follow_up() {
        let missing = order(DispatchStatus::Completed, vec![PhotoKind::Arrival]);
        let documented = order(DispatchStatus::Completed, vec![PhotoKind::Completion]);

        assert_eq!(decide_follow_up(&missing), FollowUp::RequestCompletionPhoto);
        assert_eq!(decide_follow_up(&documented), FollowUp::None);
    }
}
