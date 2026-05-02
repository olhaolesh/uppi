import { startTransition, useDeferredValue, useState } from "react";

import { ApiError } from "../api/client";
import { bulkImportClients } from "../api/clients";
import type { BulkImportResponse } from "../types/api";

function mapBulkImportError(error: unknown): string {
  if (error instanceof ApiError) {
    switch (error.status) {
      case 401:
        return "Веб-сесія завершилась. Увійдіть знову перед запуском bulk import.";
      case 422:
        return "CSV порожній або не пройшов schema validation.";
      case 400:
        return "CSV не містить жодного валідного унікального LOCATORE_CF або має невалідний формат.";
      case 500:
        return "Не вдалося обробити web-run CSV. Спробуйте ще раз.";
      case 503:
        return "Import-only runner тимчасово недоступний.";
      default:
        return error.message || "Не вдалося виконати bulk import.";
    }
  }

  if (error instanceof Error) {
    return error.message || "Не вдалося виконати bulk import.";
  }

  return "Не вдалося виконати bulk import.";
}

async function readFileText(file: File): Promise<string> {
  if (typeof file.text === "function") {
    return file.text();
  }

  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(typeof reader.result === "string" ? reader.result : "");
    reader.onerror = () => reject(reader.error ?? new Error("Не вдалося прочитати CSV файл."));
    reader.readAsText(file, "utf-8");
  });
}

export function useBulkImportFlow() {
  const [csvContent, setCsvContent] = useState("");
  const [forceUpdateVisura, setForceUpdateVisura] = useState(false);
  const [failFast, setFailFast] = useState(false);
  const [selectedFileName, setSelectedFileName] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<BulkImportResponse | null>(null);

  const deferredCsvContent = useDeferredValue(csvContent);
  const hasCsvContent = deferredCsvContent.trim().length > 0;
  const canSubmit = hasCsvContent && !loading;

  async function loadCsvFile(file: File | null) {
    if (!file) {
      return;
    }

    try {
      const text = await readFileText(file);
      startTransition(() => {
        setSelectedFileName(file.name);
        setCsvContent(text);
        setError(null);
      });
    } catch (caughtError) {
      startTransition(() => {
        setError(caughtError instanceof Error ? caughtError.message : "Не вдалося прочитати CSV файл.");
      });
    }
  }

  async function submitImport() {
    if (!csvContent.trim()) {
      setError("CSV content не може бути порожнім.");
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const response = await bulkImportClients({
        csv_content: csvContent,
        force_update_visura: forceUpdateVisura,
        fail_fast: failFast,
      });

      startTransition(() => {
        setResult(response);
      });
    } catch (caughtError) {
      startTransition(() => {
        setError(mapBulkImportError(caughtError));
      });
    } finally {
      setLoading(false);
    }
  }

  return {
    csvContent,
    setCsvContent,
    forceUpdateVisura,
    setForceUpdateVisura,
    failFast,
    setFailFast,
    selectedFileName,
    loading,
    error,
    result,
    canSubmit,
    loadCsvFile,
    submitImport,
  };
}
