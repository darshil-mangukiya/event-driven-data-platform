from __future__ import annotations

import os

from kafka.admin import KafkaAdminClient, NewTopic

from platform_shared.kafka import TOPIC_DEFINITIONS


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="topic-bootstrap")
    existing = set(admin.list_topics())
    topics = [
        NewTopic(
            name=definition.name,
            num_partitions=definition.partitions,
            replication_factor=definition.replication_factor,
            topic_configs={"retention.ms": str(definition.retention_ms)},
        )
        for definition in TOPIC_DEFINITIONS.values()
        if definition.name not in existing
    ]
    if topics:
        admin.create_topics(new_topics=topics, validate_only=False)
    print(f"created={len(topics)} existing={len(existing)}")


if __name__ == "__main__":
    main()
