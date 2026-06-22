import json
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
    )


def publish_studies(studies: list[dict]) -> dict:
    """
    Publish each study to the Kafka topic keyed by NCTId.
    Partitions are determined by study overallStatus.
    Returns a summary dict with sent and failed counts.
    """
    producer = _build_producer()
    sent, failed = 0, 0

    for study in studies:
        protocol = study.get("protocolSection", {})
        id_module = protocol.get("identificationModule", {})
        status_module = protocol.get("statusModule", {})

        nct_id = id_module.get("nctId", "UNKNOWN")
        status = status_module.get("overallStatus", "Unknown status")

        message = {
            "nctId": nct_id,
            "overallStatus": status,
            "briefTitle": id_module.get("briefTitle"),
            "lastUpdatePostDate": status_module.get("lastUpdatePostDateStruct", {}).get("date"),
            "raw": study,
        }

        try:
            future = producer.send(
                topic=KAFKA_TOPIC,
                key=nct_id,
                value=message,
            )
            future.get(timeout=10)
            sent += 1
        except KafkaError as e:
            logger.error(f"Failed to publish {nct_id}: {e}")
            failed += 1

    producer.flush()
    producer.close()

    logger.info(f"Kafka publish complete — sent: {sent}, failed: {failed}")
    return {"sent": sent, "failed": failed}


if __name__ == "__main__":
    import sys
    import json

    sample_path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/studies.json"
    with open(sample_path) as f:
        data = json.load(f)
    result = publish_studies(data.get("studies", []))
    print(result)
