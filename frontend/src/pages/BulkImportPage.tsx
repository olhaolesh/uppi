import { useState } from "react";

import StatusPanel from "../components/StatusPanel";

const placeholderResults = [
  { row: 2, locatoreCf: "RSSMRA80A01H501Z", status: "Pending web wiring" },
  { row: 3, locatoreCf: "BNCLGU85C01G482K", status: "Pending web wiring" },
];

export default function BulkImportPage() {
  const [csvContent, setCsvContent] = useState(
    "LOCATORE_CF\nRSSMRA80A01H501Z\nBNCLGU85C01G482K\n",
  );
  const [forceUpdateVisura, setForceUpdateVisura] = useState(false);
  const [failFast, setFailFast] = useState(false);

  return (
    <div className="page-stack">
      <section className="hero-card">
        <div>
          <p className="eyebrow">Екран 2</p>
          <h2>Додавання клієнтів в БД</h2>
        </div>
        <p>
          Це skeleton для future bulk-import UX. У Stage 6 він не викликає
          `/clients/bulk-import` і не пише реальний web-run CSV.
        </p>
      </section>

      <section className="panel-grid panel-grid--two">
        <article className="panel-card">
          <h3>CSV input</h3>
          <div className="form-grid">
            <label className="field">
              <span>CSV content</span>
              <textarea
                rows={8}
                value={csvContent}
                onChange={(event) => setCsvContent(event.target.value)}
              />
            </label>
            <label className="checkbox-field">
              <input
                type="checkbox"
                checked={forceUpdateVisura}
                onChange={(event) => setForceUpdateVisura(event.target.checked)}
              />
              <span>Force update visura</span>
            </label>
            <label className="checkbox-field">
              <input
                type="checkbox"
                checked={failFast}
                onChange={(event) => setFailFast(event.target.checked)}
              />
              <span>Fail fast</span>
            </label>
            <button className="primary-button" type="button" disabled>
              Запустити імпорт
            </button>
          </div>
          <p className="helper-text">
            Stage 8 підключить цей submit до current bulk-import owner path без зміни його
            semantics.
          </p>
        </article>

        <StatusPanel title="Import-only boundary" tone="warning">
          <p>
            Bulk import лишається import-only. Цей екран не створює `immobili.yml`, не запускає
            generation і не викликає `prepare-by-CF`.
          </p>
        </StatusPanel>
      </section>

      <section className="panel-grid panel-grid--two">
        <article className="panel-card">
          <h3>Placeholder summary</h3>
          <div className="stats-grid">
            <div>
              <span>Total rows</span>
              <strong>3</strong>
            </div>
            <div>
              <span>Unique clients</span>
              <strong>2</strong>
            </div>
            <div>
              <span>Imported</span>
              <strong>0</strong>
            </div>
            <div>
              <span>Failed</span>
              <strong>0</strong>
            </div>
          </div>
        </article>

        <article className="panel-card">
          <h3>Placeholder results</h3>
          <table className="data-table">
            <thead>
              <tr>
                <th>Row</th>
                <th>LOCATORE_CF</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {placeholderResults.map((item) => (
                <tr key={item.row}>
                  <td>{item.row}</td>
                  <td>{item.locatoreCf}</td>
                  <td>{item.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>
      </section>
    </div>
  );
}
