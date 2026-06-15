from __future__ import annotations

import logging
import signal
import sys
from types import FrameType

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from inference_worker.backends.mock_backend import MockBackend
from inference_worker.backends.transformers_backend import TransformersBackend
from inference_worker.config import WorkerConfig, load_config
from inference_worker.events import InferenceJob, SamplingOptions, token_event
from inference_worker.kafka_io import CancellationWatcher, KafkaIO

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("inference_worker")

_STOP = False
JOBS_TOTAL = Counter("worker_jobs_total", "Total inference jobs processed by the worker")
JOBS_FAILED = Counter("worker_jobs_failed_total", "Total inference jobs that failed")
JOBS_CANCELLED = Counter("worker_jobs_cancelled_total", "Total inference jobs cancelled")
TOKENS_TOTAL = Counter("worker_tokens_total", "Total token events produced by the worker")
ACTIVE_JOBS = Gauge("worker_active_jobs", "Inference jobs currently running")
JOB_DURATION = Histogram("worker_job_duration_seconds", "End-to-end worker job duration")


def main() -> int:
    config = load_config()
    install_signal_handlers()
    start_http_server(config.metrics_port)
    backend = build_backend(config)
    kafka = KafkaIO(config)
    cancellations = CancellationWatcher(config)
    cancellations.start()
    defaults = SamplingOptions(
        temperature=config.default_temperature,
        top_p=config.default_top_p,
        top_k=config.default_top_k,
    )

    logger.info(
        "worker started worker_id=%s backend=%s metrics_port=%s",
        config.worker_id,
        config.backend,
        config.metrics_port,
    )
    try:
        while not _STOP:
            polled = kafka.poll_job()
            if polled is None:
                continue
            message, payload = polled
            job = InferenceJob.from_payload(payload, defaults)
            process_job(job, config, backend, kafka, cancellations)
            kafka.commit(message)
    finally:
        cancellations.stop()
        kafka.close()
    return 0


def process_job(
    job: InferenceJob,
    config: WorkerConfig,
    backend: object,
    kafka: KafkaIO,
    cancellations: CancellationWatcher,
) -> None:
    logger.info("processing request_id=%s max_tokens=%s", job.request_id, job.max_tokens)
    JOBS_TOTAL.inc()
    ACTIVE_JOBS.inc()
    sequence = 0
    with JOB_DURATION.time():
        kafka.publish_token_event(
            token_event(
                request_id=job.request_id,
                sequence_number=sequence,
                event_type="STARTED",
                worker_id=config.worker_id,
            )
        )
        try:
            for generated in backend.generate(
                prompt=job.prompt,
                max_tokens=job.max_tokens,
                sampling=job.sampling,
                should_cancel=lambda: cancellations.is_cancelled(job.request_id),
            ):
                if cancellations.is_cancelled(job.request_id):
                    JOBS_CANCELLED.inc()
                    kafka.publish_token_event(
                        token_event(
                            request_id=job.request_id,
                            sequence_number=sequence,
                            event_type="CANCELLED",
                            worker_id=config.worker_id,
                            error_message="cancelled by client",
                        )
                    )
                    kafka.flush()
                    return
                sequence += 1
                TOKENS_TOTAL.inc()
                kafka.publish_token_event(
                    token_event(
                        request_id=job.request_id,
                        sequence_number=sequence,
                        event_type="TOKEN",
                        worker_id=config.worker_id,
                        token=generated.text,
                        probability=generated.probability,
                    )
                )
            kafka.publish_token_event(
                token_event(
                    request_id=job.request_id,
                    sequence_number=sequence,
                    event_type="COMPLETED",
                    worker_id=config.worker_id,
                )
            )
        except Exception as exc:
            JOBS_FAILED.inc()
            logger.exception("request_id=%s failed", job.request_id)
            kafka.publish_token_event(
                token_event(
                    request_id=job.request_id,
                    sequence_number=sequence,
                    event_type="FAILED",
                    worker_id=config.worker_id,
                    error_message=str(exc),
                )
            )
        finally:
            ACTIVE_JOBS.dec()
            kafka.flush()


def build_backend(config: WorkerConfig) -> object:
    if config.backend == "transformers":
        return TransformersBackend(
            model_name=config.model_name,
            device=config.device,
            dtype=config.dtype,
        )
    return MockBackend(token_delay_s=config.mock_token_delay_s)


def install_signal_handlers() -> None:
    def handle_signal(signum: int, frame: FrameType | None) -> None:
        del signum, frame
        global _STOP
        _STOP = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)


if __name__ == "__main__":
    sys.exit(main())
