"""Create (or re-create with --reset) the Kafka topic used by the project.

Run with:  ./run.sh py producer/admin.py [--reset]
"""

import argparse
import os
import time

from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")
TOPIC = os.environ.get("KAFKA_TOPIC", "wikistreams")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true",
                        help="delete the topic first (drops old events)")
    parser.add_argument("--partitions", type=int, default=3)
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = admin.list_topics(timeout=10).topics
    if args.reset and TOPIC in existing:
        admin.delete_topics([TOPIC])[TOPIC].result()
        print(f"deleted topic {TOPIC}")
        time.sleep(3)
        existing = admin.list_topics(timeout=10).topics
    if TOPIC not in existing:
        admin.create_topics(
            [NewTopic(TOPIC, num_partitions=args.partitions,
                      replication_factor=1)]
        )[TOPIC].result()
        print(f"created topic {TOPIC} ({args.partitions} partitions)")
    else:
        print(f"topic {TOPIC} already exists")
    print("topics:", sorted(admin.list_topics(timeout=10).topics))


if __name__ == "__main__":
    main()
