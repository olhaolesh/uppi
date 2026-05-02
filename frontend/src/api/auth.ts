import { apiFetch } from "./client";
import type { AuthStatusResponse, LoginRequest, LogoutResponse } from "../types/api";

export function loginRequest(payload: LoginRequest): Promise<AuthStatusResponse> {
  return apiFetch<AuthStatusResponse>("/auth/login", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getCurrentSession(): Promise<AuthStatusResponse> {
  return apiFetch<AuthStatusResponse>("/auth/me");
}

export function logoutRequest(): Promise<LogoutResponse> {
  return apiFetch<LogoutResponse>("/auth/logout", {
    method: "POST",
  });
}
