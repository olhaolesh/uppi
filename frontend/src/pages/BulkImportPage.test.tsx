import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError } from "../api/client";
import type { BulkImportResponse } from "../types/api";
import BulkImportPage from "./BulkImportPage";

const clientsApiMocks = vi.hoisted(() => ({
  bulkImportClients: vi.fn(),
}));

vi.mock("../api/clients", () => ({
  bulkImportClients: clientsApiMocks.bulkImportClients,
}));

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const completedResponse: BulkImportResponse = {
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
  results: [
    {
      row_number: 2,
      locatore_cf: "RSSMRA80A01H501Z",
      status: "imported",
      message: "Imported successfully",
    },
    {
      row_number: 3,
      locatore_cf: "BNCLGU85C01G482K",
      status: "imported",
      message: "Imported successfully",
    },
  ],
  invalid_rows: [
    {
      row_number: 4,
      code: "missing_locatore_cf",
      message: "clients.csv row 4 is missing LOCATORE_CF",
    },
  ],
  messages: ["Bulk import completed."],
};

const abortedResponse: BulkImportResponse = {
  ...completedResponse,
  status: "aborted",
  messages: ["Stopped after first failure because fail_fast=true."],
};

describe("BulkImportPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    sessionStorage.clear();
  });

  it("renders CSV controls and keeps submit disabled for empty input", () => {
    render(<BulkImportPage />);

    expect(screen.getByLabelText("CSV content")).toBeInTheDocument();
    expect(screen.getByLabelText("CSV file")).toBeInTheDocument();
    expect(screen.getByLabelText("Force update visura")).toBeInTheDocument();
    expect(screen.getByLabelText("Fail fast")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Запустити імпорт" })).toBeDisabled();

    fireEvent.change(screen.getByLabelText("CSV content"), {
      target: { value: "   \n  " },
    });
    expect(screen.getByRole("button", { name: "Запустити імпорт" })).toBeDisabled();
  });

  it("enables submit for non-empty CSV, shows loading and posts the expected payload", async () => {
    const pendingImport = deferred<BulkImportResponse>();
    clientsApiMocks.bulkImportClients.mockReturnValue(pendingImport.promise);

    render(<BulkImportPage />);

    fireEvent.change(screen.getByLabelText("CSV content"), {
      target: { value: "LOCATORE_CF\nRSSMRA80A01H501Z\nBNCLGU85C01G482K\n" },
    });
    fireEvent.click(screen.getByLabelText("Force update visura"));
    fireEvent.click(screen.getByRole("button", { name: "Запустити імпорт" }));

    expect(clientsApiMocks.bulkImportClients).toHaveBeenCalledWith({
      csv_content: "LOCATORE_CF\nRSSMRA80A01H501Z\nBNCLGU85C01G482K\n",
      force_update_visura: true,
      fail_fast: false,
    });
    expect(screen.getByRole("button", { name: "Імпорт триває..." })).toBeDisabled();

    pendingImport.resolve(completedResponse);

    expect(await screen.findByRole("heading", { name: "Run run-123" })).toBeInTheDocument();
    expect(screen.getByDisplayValue("clients/web_bulk_import/run-123/clients.csv")).toBeInTheDocument();
    expect(screen.getByText("Bulk import completed.")).toBeInTheDocument();
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });

  it("renders summary, results and invalid rows after a successful import", async () => {
    clientsApiMocks.bulkImportClients.mockResolvedValue(completedResponse);

    render(<BulkImportPage />);

    fireEvent.change(screen.getByLabelText("CSV content"), {
      target: { value: "LOCATORE_CF\nRSSMRA80A01H501Z\nBNCLGU85C01G482K\n" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Запустити імпорт" }));

    expect(await screen.findByText("Run ID: run-123")).toBeInTheDocument();
    expect(screen.getByText("Status: completed")).toBeInTheDocument();
    const [rowResultsTable, invalidRowsTable] = screen.getAllByRole("table");
    expect(within(rowResultsTable).getByText("RSSMRA80A01H501Z")).toBeInTheDocument();
    expect(within(rowResultsTable).getAllByText("Imported successfully")).toHaveLength(2);
    expect(within(invalidRowsTable).getByText("clients.csv row 4 is missing LOCATORE_CF")).toBeInTheDocument();
  });

  it("shows aborted status as a safe warning state", async () => {
    clientsApiMocks.bulkImportClients.mockResolvedValue(abortedResponse);

    render(<BulkImportPage />);

    fireEvent.change(screen.getByLabelText("CSV content"), {
      target: { value: "LOCATORE_CF\nRSSMRA80A01H501Z\n" },
    });
    fireEvent.click(screen.getByLabelText("Fail fast"));
    fireEvent.click(screen.getByRole("button", { name: "Запустити імпорт" }));

    expect(await screen.findByText("Status: aborted")).toBeInTheDocument();
    expect(
      screen.getByText(
        "Import був зупинений згідно з current fail-fast/result semantics. Це не frontend crash.",
      ),
    ).toBeInTheDocument();
  });

  it("shows safe API errors without raw traceback text", async () => {
    clientsApiMocks.bulkImportClients.mockRejectedValue(
      new ApiError("Traceback: import runner unavailable", 503, null),
    );

    render(<BulkImportPage />);

    fireEvent.change(screen.getByLabelText("CSV content"), {
      target: { value: "LOCATORE_CF\nRSSMRA80A01H501Z\n" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Запустити імпорт" }));

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Import-only runner тимчасово недоступний.");
    expect(alert).not.toHaveTextContent("Traceback");
  });

  it("loads CSV text from a selected file without multipart upload", async () => {
    render(<BulkImportPage />);

    const file = new File(["placeholder"], "clients.csv", { type: "text/csv" });
    Object.defineProperty(file, "text", {
      value: vi.fn().mockResolvedValue("LOCATORE_CF\nRSSMRA80A01H501Z\n"),
    });

    fireEvent.change(screen.getByLabelText("CSV file"), {
      target: { files: [file] },
    });

    await waitFor(() => {
      expect(screen.getByLabelText("CSV content")).toHaveValue("LOCATORE_CF\nRSSMRA80A01H501Z\n");
    });
    expect(screen.getByText("Завантажений файл: clients.csv")).toBeInTheDocument();
    expect(clientsApiMocks.bulkImportClients).not.toHaveBeenCalled();
  });
});
