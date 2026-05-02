import { afterEach, describe, expect, it, vi } from "vitest";

import { getCurrentSession, loginRequest, logoutRequest } from "./auth";

describe("auth api client", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    localStorage.clear();
    sessionStorage.clear();
  });

  it("always sends credentials include and does not touch browser storage", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ authenticated: true, user: { username: "operator" } }), {
          status: 200,
          headers: {
            "Content-Type": "application/json",
          },
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ authenticated: true, user: { username: "operator" } }), {
          status: 200,
          headers: {
            "Content-Type": "application/json",
          },
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ authenticated: false }), {
          status: 200,
          headers: {
            "Content-Type": "application/json",
          },
        }),
      );

    vi.stubGlobal("fetch", fetchMock);

    await loginRequest({ username: "operator", password: "secret-password", pin: "1234" });
    await getCurrentSession();
    await logoutRequest();

    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(String(fetchMock.mock.calls[0][0])).toContain("/auth/login");
    expect(String(fetchMock.mock.calls[1][0])).toContain("/auth/me");
    expect(String(fetchMock.mock.calls[2][0])).toContain("/auth/logout");
    expect(fetchMock.mock.calls.every(([, init]) => init?.credentials === "include")).toBe(true);
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });
});
