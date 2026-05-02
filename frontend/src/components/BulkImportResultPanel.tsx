import type { BulkImportResponse } from "../types/api";

type BulkImportResultPanelProps = {
  result: BulkImportResponse;
};

export default function BulkImportResultPanel({ result }: BulkImportResultPanelProps) {
  return (
    <article className="panel-card">
      <div className="panel-card__header">
        <div>
          <p className="eyebrow">Bulk import result</p>
          <h3>Run {result.run_id}</h3>
        </div>
        <span className={`chip ${result.status === "aborted" ? "chip--warning" : ""}`}>
          {result.status}
        </span>
      </div>

      {result.status === "aborted" ? (
        <div className="inline-alert inline-alert--warning" role="status">
          Import був зупинений згідно з current fail-fast/result semantics. Це не frontend crash.
        </div>
      ) : null}

      <div className="detail-grid">
        <label className="field field--readonly">
          <span>Clients CSV path</span>
          <input value={result.input.clients_csv_path} readOnly aria-readonly="true" />
        </label>
        <label className="field field--readonly">
          <span>Force update visura</span>
          <input value={String(result.input.force_update_visura)} readOnly aria-readonly="true" />
        </label>
        <label className="field field--readonly">
          <span>Fail fast</span>
          <input value={String(result.input.fail_fast)} readOnly aria-readonly="true" />
        </label>
      </div>

      <h4>Summary</h4>
      <div className="stats-grid">
        <div>
          <span>Total rows</span>
          <strong>{result.summary.total_rows}</strong>
        </div>
        <div>
          <span>Valid rows</span>
          <strong>{result.summary.valid_rows}</strong>
        </div>
        <div>
          <span>Invalid rows</span>
          <strong>{result.summary.invalid_rows}</strong>
        </div>
        <div>
          <span>Unique clients</span>
          <strong>{result.summary.unique_clients}</strong>
        </div>
        <div>
          <span>Imported</span>
          <strong>{result.summary.imported_count}</strong>
        </div>
        <div>
          <span>Failed</span>
          <strong>{result.summary.failed_count}</strong>
        </div>
        <div>
          <span>Skipped</span>
          <strong>{result.summary.skipped_count}</strong>
        </div>
      </div>

      <h4>Row results</h4>
      <table className="data-table">
        <thead>
          <tr>
            <th>Row</th>
            <th>LOCATORE_CF</th>
            <th>Status</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody>
          {result.results.length > 0 ? (
            result.results.map((row) => (
              <tr key={`${row.row_number}-${row.locatore_cf}-${row.status}`}>
                <td>{row.row_number}</td>
                <td>{row.locatore_cf}</td>
                <td>{row.status}</td>
                <td>{row.message}</td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan={4}>Без row-level результатів у цьому run.</td>
            </tr>
          )}
        </tbody>
      </table>

      <h4>Invalid rows</h4>
      <table className="data-table">
        <thead>
          <tr>
            <th>Row</th>
            <th>Code</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody>
          {result.invalid_rows.length > 0 ? (
            result.invalid_rows.map((row) => (
              <tr key={`${row.row_number}-${row.code ?? "none"}`}>
                <td>{row.row_number}</td>
                <td>{row.code || "n/a"}</td>
                <td>{row.message}</td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan={3}>Невалідних CSV rows немає.</td>
            </tr>
          )}
        </tbody>
      </table>

      {result.messages.length > 0 ? (
        <div className="message-stack">
          <h4>Messages</h4>
          <ul className="bullet-list">
            {result.messages.map((message) => (
              <li key={message}>{message}</li>
            ))}
          </ul>
        </div>
      ) : (
        <p className="helper-text">
          Без додаткових backend messages. Bulk import summary показує synchronous MVP результат.
        </p>
      )}
    </article>
  );
}
