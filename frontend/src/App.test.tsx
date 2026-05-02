import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";
import { ApiError } from "./api/client";

const authApiMocks = vi.hoisted(() => ({
  getCurrentSession: vi.fn(),
  loginRequest: vi.fn(),
  logoutRequest: vi.fn(),
}));

vi.mock("./api/auth", () => ({
  getCurrentSession: authApiMocks.getCurrentSession,
  loginRequest: authApiMocks.loginRequest,
  logoutRequest: authApiMocks.logoutRequest,
}));

function renderAt(pathname: string) {
  window.history.pushState({}, "", pathname);
  render(<App />);
}

describe("frontend skeleton app", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    sessionStorage.clear();
  });

  it("renders the login page when there is no authenticated session", async () => {
    authApiMocks.getCurrentSession.mockRejectedValue(
      new ApiError("Authentication required", 401, null),
    );

    renderAt("/login");

    expect(await screen.findByRole("heading", { name: "Вхід до UPPI Web Shell" })).toBeInTheDocument();
  });

  it("renders the protected shell and navigation for an authenticated session", async () => {
    authApiMocks.getCurrentSession.mockResolvedValue({
      authenticated: true,
      user: { username: "operator" },
    });

    renderAt("/attestazioni/generate");

    expect(
      await screen.findByRole("link", { name: /Згенерувати Attestazione/i }),
    ).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Додавання клієнтів в БД/i })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Статус \/ Логи \/ Артефакти/i })).toBeInTheDocument();
  });

  it("renders the generate screen skeleton with disabled business actions", async () => {
    authApiMocks.getCurrentSession.mockResolvedValue({
      authenticated: true,
      user: { username: "operator" },
    });

    renderAt("/attestazioni/generate");

    expect(await screen.findByRole("heading", { name: "Згенерувати Attestazione" })).toBeInTheDocument();
    expect(screen.getByLabelText("Codice fiscale locatore")).toBeInTheDocument();
    expect(screen.getByLabelText("Force update visura")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Пошук / підготувати дані" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Згенерувати Attestazione" })).toBeDisabled();
  });

  it("renders the bulk import skeleton with CSV controls and disabled submit", async () => {
    authApiMocks.getCurrentSession.mockResolvedValue({
      authenticated: true,
      user: { username: "operator" },
    });

    renderAt("/clients/bulk-import");

    expect(await screen.findByRole("heading", { name: "Додавання клієнтів в БД" })).toBeInTheDocument();
    expect(screen.getByLabelText("CSV content")).toBeInTheDocument();
    expect(screen.getByLabelText("Force update visura")).toBeInTheDocument();
    expect(screen.getByLabelText("Fail fast")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Запустити імпорт" })).toBeDisabled();
  });

  it("renders the jobs screen placeholder areas", async () => {
    authApiMocks.getCurrentSession.mockResolvedValue({
      authenticated: true,
      user: { username: "operator" },
    });

    renderAt("/jobs");

    expect(await screen.findByRole("heading", { name: "Статус / Логи / Артефакти" })).toBeInTheDocument();
    expect(screen.getByText("Placeholder runs")).toBeInTheDocument();
    expect(screen.getByText("Logs / messages")).toBeInTheDocument();
    expect(screen.getByText("Artifacts")).toBeInTheDocument();
  });

  it("submits login through auth endpoints without writing password or pin into storage", async () => {
    authApiMocks.getCurrentSession.mockRejectedValue(
      new ApiError("Authentication required", 401, null),
    );
    authApiMocks.loginRequest.mockResolvedValue({
      authenticated: true,
      user: { username: "operator" },
    });

    renderAt("/login");

    fireEvent.change(await screen.findByLabelText("Username"), {
      target: { value: "operator" },
    });
    fireEvent.change(screen.getByLabelText("Password"), {
      target: { value: "secret-password" },
    });
    fireEvent.change(screen.getByLabelText("PIN"), {
      target: { value: "1234" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Увійти в shell" }));

    await waitFor(() => {
      expect(authApiMocks.loginRequest).toHaveBeenCalledWith({
        username: "operator",
        password: "secret-password",
        pin: "1234",
      });
    });
    expect(await screen.findByRole("link", { name: /Згенерувати Attestazione/i })).toBeInTheDocument();
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });
});
