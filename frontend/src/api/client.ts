const DEFAULT_API_BASE_URL = "http://localhost:8000";

function normalizedBaseUrl(): string {
  const configured = import.meta.env.VITE_UPPI_API_BASE_URL?.trim() || DEFAULT_API_BASE_URL;
  return configured.endsWith("/") ? configured : `${configured}/`;
}

function extractErrorMessage(payload: unknown, status: number): string {
  if (typeof payload === "object" && payload !== null && "detail" in payload) {
    const detail = (payload as { detail?: unknown }).detail;
    if (typeof detail === "string" && detail.trim()) {
      return detail;
    }
  }
  return `Request failed with status ${status}`;
}

export class ApiError extends Error {
  status: number;
  payload: unknown;

  constructor(message: string, status: number, payload: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.payload = payload;
  }
}

export function buildApiUrl(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  if (import.meta.env.DEV) {
    return normalizedPath;
  }
  return new URL(normalizedPath, normalizedBaseUrl()).toString();
}

export async function apiFetch<TResponse>(path: string, init: RequestInit = {}): Promise<TResponse> {
  const headers = new Headers(init.headers);
  headers.set("Accept", "application/json");

  if (init.body && !headers.has("Content-Type") && !(init.body instanceof FormData)) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetch(buildApiUrl(path), {
    ...init,
    headers,
    credentials: "include",
  });

  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : null;

  if (!response.ok) {
    throw new ApiError(extractErrorMessage(payload, response.status), response.status, payload);
  }

  return payload as TResponse;
}
