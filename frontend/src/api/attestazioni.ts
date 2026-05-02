import { apiFetch } from "./client";
import type {
  AttestazioneGenerateRequest,
  AttestazioneGenerateResponse,
  AttestazioneSearchRequest,
  AttestazioneSearchResponse,
} from "../types/api";

export function searchAttestazioni(
  payload: AttestazioneSearchRequest,
): Promise<AttestazioneSearchResponse> {
  return apiFetch<AttestazioneSearchResponse>("/attestazioni/search", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function generateAttestazione(
  payload: AttestazioneGenerateRequest,
): Promise<AttestazioneGenerateResponse> {
  return apiFetch<AttestazioneGenerateResponse>("/attestazioni/generate", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}
