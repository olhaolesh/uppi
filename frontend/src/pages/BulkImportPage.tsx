import BulkImportResultPanel from "../components/BulkImportResultPanel";
import StatusPanel from "../components/StatusPanel";
import { useBulkImportFlow } from "../hooks/useBulkImportFlow";

export default function BulkImportPage() {
  const flow = useBulkImportFlow();

  return (
    <div className="page-stack">
      <section className="hero-card">
        <div>
          <p className="eyebrow">Екран 2</p>
          <h2>Додавання клієнтів в БД</h2>
        </div>
        <p>
          Екран реально викликає `POST /clients/bulk-import`, але не створює `immobili.yml`, не
          запускає generation і не викликає `prepare-by-CF`.
        </p>
      </section>

      <section className="panel-grid panel-grid--two">
        <article className="panel-card">
          <h3>CSV input</h3>
          <div className="form-grid">
            <label className="field">
              <span>CSV content</span>
              <textarea
                rows={10}
                placeholder={"LOCATORE_CF\nRSSMRA80A01H501Z\nBNCLGU85C01G482K\n"}
                value={flow.csvContent}
                disabled={flow.loading}
                onChange={(event) => flow.setCsvContent(event.target.value)}
              />
            </label>

            <label className="field">
              <span>CSV file</span>
              <input
                type="file"
                accept=".csv,text/csv,text/plain"
                disabled={flow.loading}
                onChange={(event) => void flow.loadCsvFile(event.target.files?.[0] ?? null)}
              />
            </label>

            {flow.selectedFileName ? (
              <p className="helper-text">Завантажений файл: {flow.selectedFileName}</p>
            ) : null}

            <label className="checkbox-field">
              <input
                type="checkbox"
                checked={flow.forceUpdateVisura}
                disabled={flow.loading}
                onChange={(event) => flow.setForceUpdateVisura(event.target.checked)}
              />
              <span>Force update visura</span>
            </label>

            <label className="checkbox-field">
              <input
                type="checkbox"
                checked={flow.failFast}
                disabled={flow.loading}
                onChange={(event) => flow.setFailFast(event.target.checked)}
              />
              <span>Fail fast</span>
            </label>

            <button
              className="primary-button"
              type="button"
              disabled={!flow.canSubmit}
              onClick={() => void flow.submitImport()}
            >
              {flow.loading ? "Імпорт триває..." : "Запустити імпорт"}
            </button>
          </div>
          <div className="message-stack">
            <p className="helper-text">
              Frontend передає JSON `csv_content` у current bulk import owner path. Multipart
              upload і пряме browser/import керування тут не використовуються.
            </p>
            <div className="csv-example" aria-label="CSV format example">
              <strong>Очікуваний CSV header</strong>
              <pre>LOCATORE_CF{"\n"}RSSMRA80A01H501Z{"\n"}BNCLGU85C01G482K</pre>
            </div>
          </div>
        </article>

        <StatusPanel
          title={flow.result ? "Import result" : "Початковий стан"}
          tone={flow.result?.status === "aborted" ? "warning" : flow.result ? "success" : "info"}
        >
          {flow.result ? (
            <div className="message-stack">
              <p>Status: {flow.result.status}</p>
              <p>Run ID: {flow.result.run_id}</p>
              <p>CSV path: {flow.result.input.clients_csv_path}</p>
              <p>Imported: {flow.result.summary.imported_count}</p>
              <p>Failed: {flow.result.summary.failed_count}</p>
            </div>
          ) : (
            <p>
              Вставте або завантажте CSV, задайте опції й запустіть import. Стан зберігається
              тільки в React state; `localStorage` і `sessionStorage` не використовуються.
            </p>
          )}
        </StatusPanel>
      </section>

      {flow.error ? (
        <div className="inline-alert" role="alert">
          {flow.error}
        </div>
      ) : null}

      <section className="panel-grid panel-grid--two">
        <StatusPanel title="Import-only boundary" tone="warning">
          <div className="message-stack">
            <p>
              Bulk import оновлює або наповнює БД через current import-only owner path. Він не
              створює `immobili.yml`, не запускає DOCX generation і не викликає `prepare-by-CF`.
            </p>
            <p>
              Browser/import flow може бути використаний backend service internally, але frontend
              ним напряму не керує.
            </p>
          </div>
        </StatusPanel>

        {flow.result ? (
          <BulkImportResultPanel result={flow.result} />
        ) : (
          <StatusPanel title="Summary pending">
            <p>
              Після успішного `POST /clients/bulk-import` тут з’являться `run_id`, summary,
              row results, invalid rows і messages.
            </p>
          </StatusPanel>
        )}
      </section>
    </div>
  );
}
