export type ModelsResponse = {
  models: string[];
  default_model_id?: string;
};

export type TokenEvent = {
  request_id?: string;
  sequence_number?: number;
  token?: string;
  probability?: number;
  event_type?: string | number;
  worker_id?: string;
  error_message?: string;
  timestamp_ms?: number;
};

export type InferStatus = {
  request_id: string;
  status: string | number;
  emitted_tokens?: number;
  worker_id?: string;
  error_message?: string;
  created_at_ms?: number;
  updated_at_ms?: number;
};

export type InferRequest = {
  requestId: string;
  modelId: string;
  prompt: string;
  maxTokens: number;
};

const apiBase = import.meta.env.VITE_API_BASE_URL ?? "";

function apiUrl(path: string) {
  return `${apiBase}${path}`;
}

async function parseJsonResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const message = await readErrorMessage(response);
    throw new Error(message || `Request failed with ${response.status}`);
  }

  return response.json() as Promise<T>;
}

async function readErrorMessage(response: Response) {
  try {
    const text = await response.text();
    if (!text) {
      return "";
    }

    try {
      const json = JSON.parse(text) as { error?: string; message?: string };
      return json.error ?? json.message ?? text;
    } catch {
      return text;
    }
  } catch {
    return "";
  }
}

export async function fetchModels(signal?: AbortSignal) {
  return parseJsonResponse<ModelsResponse>(
    await fetch(apiUrl("/api/models"), {
      signal,
    }),
  );
}

export async function cancelInference(requestId: string) {
  return parseJsonResponse<{ request_id: string; accepted: boolean; message: string }>(
    await fetch(apiUrl(`/api/infer/${encodeURIComponent(requestId)}/cancel`), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ reason: "cancelled from web console" }),
    }),
  );
}

export async function fetchInferenceStatus(requestId: string) {
  return parseJsonResponse<InferStatus>(
    await fetch(apiUrl(`/api/infer/${encodeURIComponent(requestId)}/status`)),
  );
}

export async function streamInference(
  request: InferRequest,
  onEvent: (event: TokenEvent) => void,
  signal?: AbortSignal,
) {
  const response = await fetch(apiUrl("/api/infer"), {
    method: "POST",
    headers: {
      Accept: "text/event-stream",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      request_id: request.requestId,
      model_id: request.modelId,
      prompt: request.prompt,
      max_tokens: request.maxTokens,
    }),
    signal,
  });

  if (!response.ok) {
    const message = await readErrorMessage(response);
    throw new Error(message || `Inference failed with ${response.status}`);
  }

  if (!response.body) {
    throw new Error("Inference stream did not include a response body");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    buffer += decoder.decode(value, { stream: !done });
    const chunks = buffer.split(/\r?\n\r?\n/);
    buffer = chunks.pop() ?? "";

    for (const chunk of chunks) {
      for (const event of parseSseChunk(chunk)) {
        onEvent(event);
      }
    }

    if (done) {
      break;
    }
  }

  for (const event of parseSseChunk(buffer)) {
    onEvent(event);
  }
}

function parseSseChunk(chunk: string): TokenEvent[] {
  const dataLines = chunk
    .split(/\r?\n/)
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).trim());

  if (dataLines.length > 0) {
    const data = dataLines.join("\n");
    if (!data || data === "[DONE]") {
      return [];
    }

    return [JSON.parse(data) as TokenEvent];
  }

  const trimmed = chunk.trim();
  if (!trimmed || trimmed === "[DONE]") {
    return [];
  }

  return [JSON.parse(trimmed) as TokenEvent];
}
