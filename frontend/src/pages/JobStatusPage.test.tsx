import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError } from "../api/client";
import type { JobDetail, ListJobsResponse } from "../types/api";
import JobStatusPage from "./JobStatusPage";

const jobsApiMocks = vi.hoisted(() => ({
  listJobs: vi.fn(),
  getJob: vi.fn(),
}));

vi.mock("../api/jobs", () => ({
  listJobs: jobsApiMocks.listJobs,
  getJob: jobsApiMocks.getJob,
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

const jobsListResponse: ListJobsResponse = {
  jobs: [
    {
      run_id: "run-200",
      type: "clients_bulk_import",
      status: "aborted",
      created_at: "2026-05-02T12:10:00Z",
      updated_at: "2026-05-02T12:10:05Z",
      summary: {
        total_rows: 3,
        failed_count: 1,
      },
      artifact_count: 1,
      message_count: 1,
    },
    {
      run_id: "run-100",
      type: "attestazioni_generate",
      status: "completed",
      created_at: "2026-05-02T12:00:00Z",
      updated_at: "2026-05-02T12:01:00Z",
      summary: {
        requested_count: 1,
        generated_count: 1,
        failed_count: 0,
      },
      artifact_count: 2,
      message_count: 0,
    },
  ],
};

const abortedJobDetail: JobDetail = {
  run_id: "run-200",
  type: "clients_bulk_import",
  status: "aborted",
  created_at: "2026-05-02T12:10:00Z",
  updated_at: "2026-05-02T12:10:05Z",
  started_at: "2026-05-02T12:10:00Z",
  finished_at: "2026-05-02T12:10:05Z",
  actor: {
    username: "operator",
  },
  input: {
    force_update_visura: false,
    fail_fast: true,
  },
  summary: {
    total_rows: 3,
    failed_count: 1,
  },
  artifacts: [
    {
      kind: "clients_csv",
      label: "Web-run clients.csv",
      local_path: "clients/web_bulk_import/run-200/clients.csv",
      bucket: null,
      object_key: null,
      download_url: null,
    },
  ],
  events: [
    {
      timestamp: "2026-05-02T12:10:00Z",
      level: "info",
      message: "Bulk import started",
    },
    {
      timestamp: "2026-05-02T12:10:05Z",
      level: "warning",
      message: "Bulk import aborted",
    },
  ],
  messages: ["Bulk import aborted after the first failure."],
};

const generateJobDetail: JobDetail = {
  run_id: "run-100",
  type: "attestazioni_generate",
  status: "completed",
  created_at: "2026-05-02T12:00:00Z",
  updated_at: "2026-05-02T12:01:00Z",
  started_at: "2026-05-02T12:00:01Z",
  finished_at: "2026-05-02T12:01:00Z",
  actor: {
    username: "operator",
  },
  input: {
    locatore_cf: "RSSMRA80A01H501Z",
    prepared_immobili_yaml_path: "clients/web_prepare/RSSMRA80A01H501Z/immobili.yml",
  },
  summary: {
    requested_count: 1,
    generated_count: 1,
    failed_count: 0,
  },
  artifacts: [
    {
      kind: "generation_immobili_yaml",
      label: "Generation immobili.yml",
      local_path: "clients/web_generation/RSSMRA80A01H501Z/run-100/immobili.yml",
      bucket: null,
      object_key: null,
      download_url: null,
    },
    {
      kind: "attestazione_docx",
      label: "Attestazione F12 N345 S7",
      local_path: "downloads/RSSMRA80A01H501Z/doc.docx",
      bucket: "attestazioni",
      object_key: "attestazioni/RSSMRA80A01H501Z/81.docx",
      download_url: null,
    },
  ],
  events: [
    {
      timestamp: "2026-05-02T12:00:01Z",
      level: "info",
      message: "Generation started",
    },
    {
      timestamp: "2026-05-02T12:01:00Z",
      level: "info",
      message: "Generation completed",
    },
  ],
  messages: [],
};

describe("JobStatusPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    sessionStorage.clear();
  });

  it("renders loading state while jobs list is being fetched", () => {
    const pending = deferred<ListJobsResponse>();
    jobsApiMocks.listJobs.mockReturnValue(pending.promise);

    render(<JobStatusPage />);

    expect(screen.getByText("Завантаження jobs...")).toBeInTheDocument();
  });

  it("renders empty state when no jobs exist", async () => {
    jobsApiMocks.listJobs.mockResolvedValue({ jobs: [] });

    render(<JobStatusPage />);

    expect(await screen.findByText("Жодного web job ще немає.")).toBeInTheDocument();
    expect(jobsApiMocks.getJob).not.toHaveBeenCalled();
  });

  it("renders jobs list, loads job detail, refreshes and avoids fake download links", async () => {
    jobsApiMocks.listJobs
      .mockResolvedValueOnce(jobsListResponse)
      .mockResolvedValueOnce(jobsListResponse);
    jobsApiMocks.getJob
      .mockResolvedValueOnce(abortedJobDetail)
      .mockResolvedValueOnce(generateJobDetail)
      .mockResolvedValueOnce(generateJobDetail);

    render(<JobStatusPage />);

    expect(await screen.findByText("run-200")).toBeInTheDocument();
    expect(jobsApiMocks.getJob).toHaveBeenCalledWith("run-200");
    expect(await screen.findByRole("heading", { name: "Run run-200" })).toBeInTheDocument();
    expect(screen.getByText("Bulk import aborted")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Download artifact" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Відкрити job run-100" }));

    expect(await screen.findByRole("heading", { name: "Run run-100" })).toBeInTheDocument();
    expect(screen.getByText("Generation completed")).toBeInTheDocument();
    const artifactsTable = screen.getAllByRole("table")[4];
    expect(within(artifactsTable).getByText("Attestazione F12 N345 S7")).toBeInTheDocument();
    expect(within(artifactsTable).queryByRole("link", { name: "Download artifact" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));

    await waitFor(() => {
      expect(jobsApiMocks.listJobs).toHaveBeenCalledTimes(2);
    });
  });

  it("shows safe API errors without traceback leakage", async () => {
    jobsApiMocks.listJobs.mockRejectedValue(new ApiError("Traceback: boom", 503, null));

    render(<JobStatusPage />);

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Не вдалося завантажити jobs.");
    expect(alert).not.toHaveTextContent("Traceback");
  });
});
