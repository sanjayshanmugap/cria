# Reproducible Results

This document separates distributed-system transport measurements from model
inference measurements. The mock backend uses the complete production data
path—gRPC, Kafka request routing, Python workers, Kafka token events, and
gateway streaming—but replaces model compute with a deterministic 30 ms token
delay. Its results characterize orchestration, queueing, and scaling behavior;
they do not measure language-model quality or GPU performance.

## Mock transport baseline

Measured July 11, 2026 at commit `bc97065` on an Apple M4 MacBook Pro
(`Mac16,1`, 24 GB RAM), Docker Engine 28.5.1, and Docker Compose 2.40.2.
Each cell contains nine requests (three fixed prompts repeated three times),
16 output tokens per request, three Kafka partitions, and a clean Kafka volume.
The success rate was 100% for all 72 measured requests.

| Workers | Concurrency | TTFT p50 | TTFT p95 | Total p95 | Tokens/s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 57.9 ms | 60.2 ms | 555.6 ms | 29.46 |
| 1 | 2 | 580.9 ms | 600.1 ms | 1,097.7 ms | 29.82 |
| 1 | 4 | 1,652.3 ms | 1,698.1 ms | 2,196.4 ms | 29.71 |
| 1 | 8 | 2,159.3 ms | 3,759.7 ms | 4,254.9 ms | 29.98 |
| 2 | 1 | 52.5 ms | 55.0 ms | 548.5 ms | 29.66 |
| 2 | 2 | 50.7 ms | 379.1 ms | 867.3 ms | 53.57 |
| 2 | 4 | 1,084.4 ms | 1,618.6 ms | 2,109.5 ms | 38.95 |
| 2 | 8 | 1,103.7 ms | 2,167.1 ms | 2,657.4 ms | 45.03 |

At matched concurrency two, adding a second worker increased throughput from
29.82 to 53.57 tokens/s (1.80x). At higher concurrency, Kafka consumer
scheduling and the intentionally serial worker execution increase TTFT; this
is visible rather than hidden by aggregate throughput.

## Reproduce the mock baseline

Start from a clean volume so retained requests and consumer offsets cannot
contaminate latency:

```bash
docker compose down -v
docker compose up --build -d --wait --scale worker=1
mkdir -p benchmark-results

for workers in 1 2; do
  docker compose up -d --wait --scale worker="$workers" worker
  python scripts/smoke_test.py --max-tokens 4 --skip-cancel
  for parallel in 1 2 4 8; do
    (
      cd workers/python
      python load_test.py \
        --parallel "$parallel" \
        --max-tokens 16 \
        --repetitions 3 \
        --worker-replicas "$workers" \
        --output "../../benchmark-results/mock-w${workers}-c${parallel}.json"
    )
  done
done

python scripts/summarize_benchmarks.py benchmark-results \
  --output benchmark-results/summary.md
```

The nightly GitHub Actions workflow runs the same matrix and retains both the
JSON reports and Markdown summary for 30 days.

## TinyLLaMA baseline

TinyLLaMA results must be reported separately because hardware, dtype, device,
and model download state dominate latency. Use the optional Compose profile and
record those fields alongside the generated JSON:

```bash
docker compose --profile llm up --build -d --wait
cd workers/python
python load_test.py \
  --model-id tinyllama-1.1b-chat \
  --backend transformers \
  --parallel 1 \
  --max-tokens 32 \
  --repetitions 3 \
  --worker-replicas 1 \
  --output ../../benchmark-results/tinyllama-local.json
```

No TinyLLaMA number is checked in yet: the current public baseline was measured
on the mock path, and inventing or comparing real-model performance without a
stable device/dtype configuration would be misleading.

## Interpretation limits

- The sample size is deliberately small enough for free CI; use larger prompt
  sets and repetitions for capacity planning.
- Token events are at-least-once. Clients should de-duplicate by request ID and
  sequence number.
- Results from GitHub-hosted runners and developer laptops are not directly
  comparable; every JSON artifact includes OS, architecture, CPU count, git
  SHA, topology, and benchmark parameters.
- The mock backend isolates distributed-system behavior. It says nothing about
  model accuracy, perplexity, or generation quality.
