import { afterEach, describe, expect, it, vi } from "vitest";

import { bulkImportClients } from "./clients";

describe("clients api client", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    localStorage.clear();
    sessionStorage.clear();
  });

  it("posts bulk import requests with credentials include", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          status: "completed",
          run_id: "run-123",
          input: {
            clients_csv_path: "clients/web_bulk_import/run-123/clients.csv",
            force_update_visura: true,
            fail_fast: false,
          },
          summary: {
            total_rows: 3,
            valid_rows: 2,
            invalid_rows: 1,
            unique_clients: 2,
            imported_count: 2,
            failed_count: 0,
            skipped_count: 0,
          },
          results: [],
          invalid_rows: [],
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

    await bulkImportClients({
      csv_content: "LOCATORE_CF\nRSSMRA80A01H501Z\n",
      force_update_visura: true,
      fail_fast: false,
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(String(fetchMock.mock.calls[0][0])).toContain("/clients/bulk-import");
    expect(fetchMock.mock.calls[0][1]?.credentials).toBe("include");
    expect(JSON.parse(String(fetchMock.mock.calls[0][1]?.body))).toEqual({
      csv_content: "LOCATORE_CF\nRSSMRA80A01H501Z\n",
      force_update_visura: true,
      fail_fast: false,
    });
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });
});
