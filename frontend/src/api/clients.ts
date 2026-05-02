import type { BulkImportRequest, BulkImportResponse } from "../types/api";
import { apiFetch } from "./client";

export function bulkImportClients(payload: BulkImportRequest): Promise<BulkImportResponse> {
  return apiFetch<BulkImportResponse>("/clients/bulk-import", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}
