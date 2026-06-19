import { FormEvent, useEffect, useMemo, useRef, useState } from "react";

import { Button } from "@/components/ui/Button";
import { Card } from "@/components/ui/Card";
import { Select } from "@/components/ui/Select";
import { Textarea } from "@/components/ui/Textarea";
import {
  cancelInference,
  fetchInferenceStatus,
  fetchModels,
  streamInference,
  type InferStatus,
  type TokenEvent,
} from "@/lib/api";
import { cn } from "@/lib/utils";

const DEFAULT_PROMPT =
  "Explain durable Kafka token streaming in a distributed LLM inference engine.";

const terminalEventTypes = new Set([
  3,
  4,
  5,
  "completed",
  "failed",
  "cancelled",
  "TOKEN_EVENT_TYPE_COMPLETED",
  "TOKEN_EVENT_TYPE_FAILED",
  "TOKEN_EVENT_TYPE_CANCELLED",
]);

function App() {
  const [models, setModels] = useState<string[]>([]);
  const [selectedModel, setSelectedModel] = useState("");
  const [modelsState, setModelsState] = useState<
    "loading" | "ready" | "empty" | "error"
  >("loading");
  const [modelsError, setModelsError] = useState("");
  const [prompt, setPrompt] = useState(DEFAULT_PROMPT);
  const [maxTokens, setMaxTokens] = useState(64);
  const [requestId, setRequestId] = useState("");
  const [output, setOutput] = useState("");
  const [events, setEvents] = useState<TokenEvent[]>([]);
  const [streamState, setStreamState] = useState<
    "idle" | "streaming" | "completed" | "cancelled" | "error"
  >("idle");
  const [streamError, setStreamError] = useState("");
  const [status, setStatus] = useState<InferStatus | null>(null);
  const [statusState, setStatusState] = useState<"idle" | "loading" | "error">(
    "idle",
  );
  const [statusError, setStatusError] = useState("");
  const streamAbortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    const controller = new AbortController();

    fetchModels(controller.signal)
      .then((response) => {
        setModels(response.models);
        setSelectedModel(response.default_model_id ?? response.models[0] ?? "");
        setModelsState(response.models.length > 0 ? "ready" : "empty");
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) {
          return;
        }

        setModelsError(error instanceof Error ? error.message : "Unable to load models");
        setModelsState("error");
      });

    return () => controller.abort();
  }, []);

  const latestEvent = events.at(-1);
  const isStreaming = streamState === "streaming";
  const canSubmit =
    modelsState === "ready" &&
    selectedModel.length > 0 &&
    prompt.trim().length > 0 &&
    !isStreaming;
  const canCheckStatus = requestId.length > 0 && !isStreaming;

  const statusLabel = useMemo(() => {
    if (streamState === "idle") {
      return "Ready";
    }

    if (streamState === "streaming") {
      return "Streaming";
    }

    if (streamState === "completed") {
      return "Completed";
    }

    if (streamState === "cancelled") {
      return "Cancelled";
    }

    return "Error";
  }, [streamState]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canSubmit) {
      return;
    }

    const nextRequestId = crypto.randomUUID();
    const controller = new AbortController();
    streamAbortRef.current = controller;

    setRequestId(nextRequestId);
    setOutput("");
    setEvents([]);
    setStatus(null);
    setStatusError("");
    setStreamError("");
    setStreamState("streaming");

    try {
      await streamInference(
        {
          requestId: nextRequestId,
          modelId: selectedModel,
          prompt: prompt.trim(),
          maxTokens,
        },
        (tokenEvent) => {
          setEvents((current) => [...current, tokenEvent]);
          if (tokenEvent.token) {
            setOutput((current) => `${current}${tokenEvent.token}`);
          }
          if (tokenEvent.request_id) {
            setRequestId(tokenEvent.request_id);
          }
          if (isTerminalEvent(tokenEvent)) {
            setStreamState(eventToStreamState(tokenEvent));
          }
        },
        controller.signal,
      );

      setStreamState((current) => (current === "streaming" ? "completed" : current));
    } catch (error: unknown) {
      if (controller.signal.aborted) {
        return;
      }

      setStreamError(error instanceof Error ? error.message : "Inference failed");
      setStreamState("error");
    } finally {
      streamAbortRef.current = null;
    }
  }

  async function handleCancel() {
    if (!requestId || !isStreaming) {
      return;
    }

    try {
      await cancelInference(requestId);
      streamAbortRef.current?.abort();
      setStreamState("cancelled");
    } catch (error: unknown) {
      setStreamError(error instanceof Error ? error.message : "Cancel failed");
      setStreamState("error");
    }
  }

  async function handleStatus() {
    if (!requestId) {
      return;
    }

    setStatusState("loading");
    setStatusError("");

    try {
      setStatus(await fetchInferenceStatus(requestId));
      setStatusState("idle");
    } catch (error: unknown) {
      setStatusError(error instanceof Error ? error.message : "Status lookup failed");
      setStatusState("error");
    }
  }

  return (
    <main className="min-h-screen px-4 py-6 sm:px-6 lg:px-8">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-6">
        <header className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
          <div className="space-y-3">
            <p className="text-sm font-medium uppercase tracking-[0.2em] text-[hsl(var(--accent))]">
              Cria Phase 4
            </p>
            <div className="space-y-2">
              <h1 className="max-w-3xl text-3xl font-semibold tracking-tight text-[hsl(var(--foreground))] sm:text-4xl">
                Web console for streaming inference
              </h1>
              <p className="max-w-2xl text-base leading-7 text-[hsl(var(--muted-foreground))]">
                Submit prompts to the Rust BFF, watch token events arrive over
                SSE, and keep control with status and cancellation actions.
              </p>
            </div>
          </div>
          <StatusPill state={streamState} label={statusLabel} />
        </header>

        <div className="grid gap-6 lg:grid-cols-[minmax(0,420px)_1fr]">
          <Card className="p-4 sm:p-6">
            <form className="flex h-full flex-col gap-5" onSubmit={handleSubmit}>
              <div className="space-y-2">
                <label
                  className="text-sm font-medium text-[hsl(var(--foreground))]"
                  htmlFor="model"
                >
                  Model
                </label>
                <Select
                  disabled={modelsState !== "ready" || isStreaming}
                  id="model"
                  onChange={(event) => setSelectedModel(event.target.value)}
                  value={selectedModel}
                >
                  {modelsState === "loading" && <option>Loading models...</option>}
                  {modelsState === "empty" && <option>No models available</option>}
                  {modelsState === "error" && <option>Models unavailable</option>}
                  {models.map((model) => (
                    <option key={model} value={model}>
                      {model}
                    </option>
                  ))}
                </Select>
                <ModelStateMessage state={modelsState} error={modelsError} />
              </div>

              <div className="space-y-2">
                <label
                  className="text-sm font-medium text-[hsl(var(--foreground))]"
                  htmlFor="prompt"
                >
                  Prompt
                </label>
                <Textarea
                  disabled={isStreaming}
                  id="prompt"
                  onChange={(event) => setPrompt(event.target.value)}
                  placeholder="Ask the inference engine something..."
                  value={prompt}
                />
              </div>

              <div className="grid gap-4 sm:grid-cols-[1fr_auto] sm:items-end">
                <div className="space-y-2">
                  <label
                    className="text-sm font-medium text-[hsl(var(--foreground))]"
                    htmlFor="max-tokens"
                  >
                    Max tokens
                  </label>
                  <input
                    className="min-h-11 w-full rounded-xl border border-[hsl(var(--input))] bg-white px-3 py-2 text-sm transition-colors duration-200 ease-out hover:border-[hsl(var(--muted-foreground)/0.45)] disabled:bg-[hsl(var(--muted))]"
                    disabled={isStreaming}
                    id="max-tokens"
                    max={512}
                    min={1}
                    onChange={(event) => setMaxTokens(Number(event.target.value))}
                    type="number"
                    value={maxTokens}
                  />
                </div>
                <Button className="w-full sm:w-auto" disabled={!canSubmit} type="submit">
                  {isStreaming ? "Streaming..." : "Run inference"}
                </Button>
              </div>

              {streamError && (
                <div className="rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
                  {streamError}
                </div>
              )}
            </form>
          </Card>

          <Card className="flex min-h-[520px] flex-col overflow-hidden">
            <div className="flex flex-col gap-3 border-b border-[hsl(var(--border))] p-4 sm:flex-row sm:items-center sm:justify-between sm:p-6">
              <div>
                <h2 className="text-lg font-semibold text-[hsl(var(--foreground))]">
                  Stream output
                </h2>
                <p className="mt-1 text-sm text-[hsl(var(--muted-foreground))]">
                  {requestId ? `Request ${requestId}` : "No active request yet"}
                </p>
              </div>
              <div className="flex flex-col gap-2 sm:flex-row">
                <Button
                  disabled={!canCheckStatus || statusState === "loading"}
                  onClick={handleStatus}
                  variant="secondary"
                >
                  {statusState === "loading" ? "Checking..." : "Check status"}
                </Button>
                <Button disabled={!isStreaming || !requestId} onClick={handleCancel} variant="danger">
                  Cancel
                </Button>
              </div>
            </div>

            <div className="flex flex-1 flex-col gap-4 p-4 sm:p-6">
              <OutputPanel
                error={streamError}
                isStreaming={isStreaming}
                output={output}
                state={streamState}
              />

              <div className="grid gap-3 md:grid-cols-2">
                <MetadataItem label="Latest event" value={formatEventType(latestEvent)} />
                <MetadataItem
                  label="Tokens emitted"
                  value={String(status?.emitted_tokens ?? countTokenEvents(events))}
                />
                <MetadataItem label="Worker" value={status?.worker_id || latestEvent?.worker_id || "Pending"} />
                <MetadataItem label="Status" value={formatStatus(status?.status) || statusLabel} />
              </div>

              {statusError && (
                <div className="rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
                  {statusError}
                </div>
              )}
              {status?.error_message && (
                <div className="rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
                  {status.error_message}
                </div>
              )}
            </div>
          </Card>
        </div>
      </div>
    </main>
  );
}

function StatusPill({
  state,
  label,
}: {
  state: "idle" | "streaming" | "completed" | "cancelled" | "error";
  label: string;
}) {
  return (
    <div
      className={cn(
        "inline-flex w-fit items-center gap-2 rounded-full border px-3 py-2 text-sm font-medium",
        state === "streaming" &&
          "border-[hsl(var(--accent)/0.3)] bg-[hsl(var(--accent)/0.08)] text-[hsl(var(--accent))]",
        state === "completed" &&
          "border-emerald-200 bg-emerald-50 text-emerald-700",
        state === "cancelled" &&
          "border-amber-200 bg-amber-50 text-amber-700",
        state === "error" && "border-red-200 bg-red-50 text-red-700",
        state === "idle" &&
          "border-[hsl(var(--border))] bg-[hsl(var(--card))] text-[hsl(var(--muted-foreground))]",
      )}
    >
      <span
        className={cn(
          "h-2 w-2 rounded-full bg-current",
          state === "streaming" && "animate-pulse",
        )}
      />
      {label}
    </div>
  );
}

function ModelStateMessage({
  state,
  error,
}: {
  state: "loading" | "ready" | "empty" | "error";
  error: string;
}) {
  if (state === "ready") {
    return null;
  }

  const copy = {
    loading: "Loading available models from /api/models.",
    empty: "No models were returned. Start a worker or update the BFF model routes.",
    error: error || "Could not load models from the BFF.",
  }[state];

  return <p className="text-sm text-[hsl(var(--muted-foreground))]">{copy}</p>;
}

function OutputPanel({
  output,
  state,
  isStreaming,
  error,
}: {
  output: string;
  state: "idle" | "streaming" | "completed" | "cancelled" | "error";
  isStreaming: boolean;
  error: string;
}) {
  if (state === "idle") {
    return (
      <div className="flex min-h-64 flex-1 items-center justify-center rounded-3xl border border-dashed border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.5)] p-8 text-center">
        <p className="max-w-sm text-sm leading-6 text-[hsl(var(--muted-foreground))]">
          Submit a prompt to begin. Token events will appear here as the BFF
          streams them.
        </p>
      </div>
    );
  }

  if (state === "error" && !output) {
    return (
      <div className="flex min-h-64 flex-1 items-center justify-center rounded-3xl border border-red-200 bg-red-50 p-8 text-center">
        <p className="max-w-sm text-sm leading-6 text-red-800">
          {error || "The inference request failed before any tokens arrived."}
        </p>
      </div>
    );
  }

  return (
    <pre className="min-h-64 flex-1 whitespace-pre-wrap rounded-3xl border border-[hsl(var(--border))] bg-[hsl(var(--foreground))] p-5 text-sm leading-7 text-white shadow-inner">
      {output}
      {isStreaming && <span className="ml-1 inline-block animate-pulse">▌</span>}
    </pre>
  );
}

function MetadataItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.4)] px-4 py-3">
      <p className="text-xs font-medium uppercase tracking-[0.16em] text-[hsl(var(--muted-foreground))]">
        {label}
      </p>
      <p className="mt-1 truncate text-sm font-medium text-[hsl(var(--foreground))]">
        {value}
      </p>
    </div>
  );
}

function isTerminalEvent(event: TokenEvent) {
  return terminalEventTypes.has(event.event_type ?? "");
}

function eventToStreamState(event: TokenEvent) {
  const eventType = event.event_type;
  if (eventType === 4 || eventType === "failed" || eventType === "TOKEN_EVENT_TYPE_FAILED") {
    return "error";
  }
  if (
    eventType === 5 ||
    eventType === "cancelled" ||
    eventType === "TOKEN_EVENT_TYPE_CANCELLED"
  ) {
    return "cancelled";
  }

  return "completed";
}

function formatEventType(event?: TokenEvent) {
  if (!event) {
    return "Waiting";
  }

  return formatStatus(event.event_type) || "Event";
}

function formatStatus(value?: string | number) {
  if (value === undefined || value === "") {
    return "";
  }

  if (typeof value === "number") {
    const statuses: Record<number, string> = {
      0: "Unspecified",
      1: "Queued",
      2: "Running",
      3: "Completed",
      4: "Failed",
      5: "Cancelled",
    };
    return statuses[value] ?? `Code ${value}`;
  }

  return value
    .replace(/^TOKEN_EVENT_TYPE_/, "")
    .replace(/^REQUEST_STATUS_/, "")
    .toLowerCase()
    .replace(/(^|_)([a-z])/g, (_, prefix: string, char: string) =>
      `${prefix ? " " : ""}${char.toUpperCase()}`,
    );
}

function countTokenEvents(events: TokenEvent[]) {
  return events.filter((event) => event.token).length;
}

export default App;
