import StatusPanel from "../components/StatusPanel";

const placeholderRuns = [
  {
    runId: "stage7-demo-001",
    type: "attestazioni.generate",
    status: "placeholder",
    createdAt: "2026-05-02 10:00",
    summary: "1 requested / 0 generated in Stage 6 UI",
  },
  {
    runId: "stage8-demo-001",
    type: "clients.bulk-import",
    status: "placeholder",
    createdAt: "2026-05-02 10:05",
    summary: "CSV summary model arrives in Stage 9",
  },
];

export default function JobStatusPage() {
  return (
    <div className="page-stack">
      <section className="hero-card">
        <div>
          <p className="eyebrow">Екран 3</p>
          <h2>Статус / Логи / Артефакти</h2>
        </div>
        <p>
          Stage 6 показує тільки UI-заготовку. Реальна jobs/logs/artifacts model буде окремим
          Stage 9 backend + UI slice.
        </p>
      </section>

      <section className="panel-card">
        <h3>Placeholder runs</h3>
        <table className="data-table">
          <thead>
            <tr>
              <th>run_id</th>
              <th>type</th>
              <th>status</th>
              <th>created_at</th>
              <th>summary</th>
            </tr>
          </thead>
          <tbody>
            {placeholderRuns.map((run) => (
              <tr key={run.runId}>
                <td>{run.runId}</td>
                <td>{run.type}</td>
                <td>{run.status}</td>
                <td>{run.createdAt}</td>
                <td>{run.summary}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="panel-grid panel-grid--two">
        <StatusPanel title="Logs / messages">
          <ul className="bullet-list">
            <li>Structured job logs ще не існують у backend shell.</li>
            <li>Цей блок лише фіксує майбутню зону для status/messages timeline.</li>
          </ul>
        </StatusPanel>

        <StatusPanel title="Artifacts">
          <ul className="bullet-list">
            <li>Download endpoint поки відсутній.</li>
            <li>Artifact references і signed URLs підуть разом із Stage 9 model.</li>
          </ul>
        </StatusPanel>
      </section>
    </div>
  );
}
