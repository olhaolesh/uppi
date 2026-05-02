import type { AttestazioneGenerateRequest, AttestazioneSearchRequest } from "../types/api";

export async function searchPreparedAttestazione(
  _payload: AttestazioneSearchRequest,
): Promise<never> {
  throw new Error("Stage 7 will wire the generate screen to /attestazioni/search.");
}

export async function generateAttestazione(
  _payload: AttestazioneGenerateRequest,
): Promise<never> {
  throw new Error("Stage 7 will wire the generate screen to /attestazioni/generate.");
}
