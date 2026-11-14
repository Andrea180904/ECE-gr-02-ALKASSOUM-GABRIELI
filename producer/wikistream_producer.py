"""Stream live Wikipedia changes (Wikimedia EventStreams) into Kafka.

Reads the server-sent-events endpoint /v2/stream/recentchange, keeps
article edits and page creations of the selected wikis and publishes the
raw JSON event to Kafka (key = page title). Relevance to IMDb is decided
later, in Spark (jobs/05_streaming.py).

Run with:  ./run.sh py producer/wikistream_producer.py --duration 25
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta

import requests
from confluent_kafka import Producer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")
TOPIC = os.environ.get("KAFKA_TOPIC", "wikistreams")


def sse_events(session: requests.Session, last_event_id=None):
    """Yield (event_id, data_dict) from the SSE stream."""
    headers = {"Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    with session.get(STREAM_URL, headers=headers, stream=True,
                     timeout=(10, 60)) as resp:
        resp.raise_for_status()
        event_id, data_lines = None, []
        for raw in resp.iter_lines(decode_unicode=True):
            if raw is None:
                continue
            if raw == "":  # end of one event
                if data_lines:
                    try:
                        yield event_id, json.loads("\n".join(data_lines))
                    except json.JSONDecodeError:
                        pass
                event_id, data_lines = None, []
            elif raw.startswith("data:"):
                data_lines.append(raw[5:].lstrip())
            elif raw.startswith("id:"):
                event_id = raw[3:].strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=25,
                        help="minutes to run")
    parser.add_argument("--wikis", default="enwiki,frwiki",
                        help="comma separated wiki ids to keep")
    parser.add_argument("--contact", default="student@ece.fr",
                        help="e-mail put in the User-Agent")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    wikis = set(args.wikis.split(","))

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "wikistream-producer",
        "compression.type": "lz4",
        "linger.ms": 50,
    })
    session = requests.Session()
    session.headers["User-Agent"] = (
        f"ECE-BigData-StudentProject/1.0 (contact: {args.contact})")

    stop = datetime.now() + timedelta(minutes=args.duration)
    seen = sent = 0
    last_id, backoff, last_report = None, 2, time.monotonic()
    print(f"streaming {sorted(wikis)} -> kafka {BOOTSTRAP}/{TOPIC} "
          f"until {stop:%H:%M:%S}")
    while datetime.now() < stop:
        try:
            for event_id, change in sse_events(session, last_id):
                last_id = event_id or last_id
                seen += 1
                if (change.get("wiki") in wikis
                        and change.get("namespace") == 0
                        and change.get("type") in ("edit", "new")):
                    producer.produce(
                        TOPIC,
                        key=change.get("title", "").encode("utf-8"),
                        value=json.dumps(change).encode("utf-8"),
                    )
                    producer.poll(0)
                    sent += 1
                    if args.verbose:
                        print(f"{change['wiki']}: {change['title']}")
                if time.monotonic() - last_report > 30:
                    print(f"{datetime.now():%H:%M:%S} seen={seen:,} "
                          f"sent={sent:,}")
                    last_report = time.monotonic()
                if datetime.now() >= stop:
                    break
            backoff = 2
        except (requests.RequestException, ConnectionError) as exc:
            print(f"stream interrupted ({exc}); reconnect in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
    producer.flush(30)
    print(f"done: seen={seen:,} events, sent={sent:,} to Kafka")


if __name__ == "__main__":
    main()
