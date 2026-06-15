from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, Producer

from inference_worker.config import WorkerConfig

logger = logging.getLogger(__name__)


class KafkaIO:
    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.consumer = Consumer(
            {
                "bootstrap.servers": config.kafka_brokers,
                "group.id": config.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.consumer.subscribe([config.request_topic])
        self.producer = Producer({"bootstrap.servers": config.kafka_brokers})

    def poll_job(self) -> tuple[Any, dict[str, Any]] | None:
        message = self.consumer.poll(self.config.poll_timeout_s)
        if message is None:
            return None
        if message.error():
            logger.warning("Kafka consumer error: %s", message.error())
            return None
        payload = json.loads(message.value().decode("utf-8"))
        return message, payload

    def publish_token_event(self, event: dict[str, Any]) -> None:
        request_id = event["request_id"]
        self.producer.produce(
            self.config.token_topic,
            key=request_id.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )
        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush(10)

    def commit(self, message: Any) -> None:
        self.consumer.commit(message=message, asynchronous=False)

    def close(self) -> None:
        self.consumer.close()


class CancellationWatcher:
    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self._cancelled: set[str] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._consumer = Consumer(
            {
                "bootstrap.servers": config.kafka_brokers,
                "group.id": f"{config.group_id}-{config.worker_id}-control",
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
        )

    def start(self) -> None:
        self._consumer.subscribe([self.config.control_topic])
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        self._consumer.close()

    def is_cancelled(self, request_id: str) -> bool:
        with self._lock:
            return request_id in self._cancelled

    def _run(self) -> None:
        while not self._stop.is_set():
            message = self._consumer.poll(1.0)
            if message is None or message.error():
                continue
            try:
                payload = json.loads(message.value().decode("utf-8"))
                request_id = str(payload["request_id"])
            except Exception:
                logger.exception("invalid cancellation event")
                continue
            with self._lock:
                self._cancelled.add(request_id)
