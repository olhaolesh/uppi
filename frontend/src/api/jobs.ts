import type { JobStatus, JobType, JobDetail, ListJobsResponse } from "../types/api";
import { apiFetch } from "./client";

export type ListJobsParams = {
  type?: JobType | "";
  status?: JobStatus | "";
  limit?: number;
};

export function listJobs(params: ListJobsParams = {}): Promise<ListJobsResponse> {
  const search = new URLSearchParams();
  if (params.type) {
    search.set("type", params.type);
  }
  if (params.status) {
    search.set("status", params.status);
  }
  if (typeof params.limit === "number") {
    search.set("limit", String(params.limit));
  }

  const suffix = search.size > 0 ? `?${search.toString()}` : "";
  return apiFetch<ListJobsResponse>(`/jobs${suffix}`);
}

export function getJob(runId: string): Promise<JobDetail> {
  return apiFetch<JobDetail>(`/jobs/${encodeURIComponent(runId)}`);
}
