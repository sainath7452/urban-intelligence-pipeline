# pubsub_publisher.py
# Simulates real-time ride booking events published to GCP Pub/Sub
# In production this would be triggered by an actual booking system

import json
import random
import time
from datetime import datetime

from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = "urban-intelligence-pipeline-sv"
TOPIC_ID = "ride-events"

# NYC pickup location IDs from the taxi dataset
LOCATION_IDS = [
    "132", "161", "237", "186", "170", "48", "68", "107",
    "100", "142", "239", "141", "229", "263", "234", "114"
]

PAYMENT_TYPES = ["credit_card", "cash", "no_charge", "dispute"]
VENDOR_IDS = [1, 2]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def generate_ride_event() -> dict:
    """
    Generate a synthetic ride booking event.
    Mimics the structure of real NYC taxi trip data.
    """
    pickup_location = random.choice(LOCATION_IDS)
    dropoff_location = random.choice(LOCATION_IDS)
    passenger_count = random.randint(1, 4)
    trip_distance = round(random.uniform(0.5, 25.0), 2)
    fare_amount = round(trip_distance * random.uniform(2.5, 4.0) + 3.0, 2)
    tip_amount = round(fare_amount * random.uniform(0, 0.25), 2)

    event = {
        "event_id": f"evt_{datetime.now().strftime('%Y%m%d%H%M%S')}_{random.randint(1000, 9999)}",
        "event_type": "ride_booking",
        "timestamp": datetime.now().isoformat(),
        "vendor_id": random.choice(VENDOR_IDS),
        "passenger_count": passenger_count,
        "trip_distance": trip_distance,
        "pickup_location_id": pickup_location,
        "dropoff_location_id": dropoff_location,
        "payment_type": random.choice(PAYMENT_TYPES),
        "fare_amount": fare_amount,
        "tip_amount": tip_amount,
        "total_amount": round(fare_amount + tip_amount + 0.5, 2),
        "source_system": "synthetic_publisher",
        "ingestion_timestamp": datetime.now().isoformat()
    }
    return event


def publish_event(event: dict) -> str:
    """
    Publish a single ride event to Pub/Sub topic.
    Returns the message ID assigned by Pub/Sub.
    """
    message_bytes = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, message_bytes)
    return future.result()


def run_publisher(num_events: int = 100, delay_seconds: float = 0.1):
    """
    Publish a stream of synthetic ride events to Pub/Sub.
    delay_seconds controls the rate of event generation.
    """
    print(f"Starting Pub/Sub publisher")
    print(f"Topic: {topic_path}")
    print(f"Publishing {num_events} events...")

    published = 0
    failed = 0

    for i in range(num_events):
        try:
            event = generate_ride_event()
            message_id = publish_event(event)
            published += 1

            if published % 10 == 0:
                print(f"Published {published}/{num_events} events")

            time.sleep(delay_seconds)

        except Exception as e:
            print(f"Failed to publish event {i}: {e}")
            failed += 1

    print(f"Publishing complete")
    print(f"Published: {published}")
    print(f"Failed: {failed}")


if __name__ == "__main__":
    run_publisher(num_events=100, delay_seconds=0.1)