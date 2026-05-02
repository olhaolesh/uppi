import type { BulkImportRequest } from "../types/api";

export async function runBulkImport(_payload: BulkImportRequest): Promise<never> {
  throw new Error("Stage 8 will wire the bulk-import screen to /clients/bulk-import.");
}
