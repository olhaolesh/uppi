import { afterEach, describe, expect, it, vi } from "vitest";

import { getJob, listJobs } from "./jobs";

describe("jobs api client", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    localStorage.clear();
    sessionStorage.clear();
  });

  it("loads jobs list and detail with credentials include", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            jobs: [
              {
                run_id: "run-123",
                type: "attestazioni_generate",
                status: "completed",
                created_at: "2026-05-02T12:00:00Z",
                updated_at: "2026-05-02T12:01:00Z",
                summary: {
                  requested_count: 1,
                  generated_count: 1,
                  failed_count: 0,
                },
                artifact_count: 1,
                message_count: 0,
              },
            ],
          }),
          {
            status: 200,
            headers: {
              "Content-Type": "application/json",
            },
          },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            run_id: "run-123",
            type: "attestazioni_generate",
            status: "completed",
            created_at: "2026-05-02T12:00:00Z",
            updated_at: "2026-05-02T12:01:00Z",
            started_at: "2026-05-02T12:00:01Z",
            finished_at: "2026-05-02T12:01:00Z",
            actor: { username: "operator" },
            input: { locatore_cf: "RSSMRA80A01H501Z" },
            summary: {
              requested_count: 1,
              generated_count: 1,
              failed_count: 0,
            },
            artifacts: [],
            events: [],
            messages: [],
          }),
          {
            status: 200,
            headers: {
              "Content-Type": "application/json",
            },
          },
        ),
      );

    vi.stubGlobal("fetch", fetchMock);

    await listJobs({ type: "attestazioni_generate", status: "completed", limit: 25 });
    await getJob("run-123");

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(String(fetchMock.mock.calls[0][0])).toContain(
      "/jobs?type=attestazioni_generate&status=completed&limit=25",
    );
    expect(String(fetchMock.mock.calls[1][0])).toContain("/jobs/run-123");
    expect(fetchMock.mock.calls.every(([, init]) => init?.credentials === "include")).toBe(true);
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });
});
