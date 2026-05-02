import type { JobDetail } from "../types/api";

type JobDetailPanelProps = {
  job: JobDetail;
};

function renderSummary(summary: JobDetail["summary"]): string {
  const entries = Object.entries(summary);
  if (entries.length === 0) {
    return "n/a";
  }
  return entries.map(([key, value]) => `${key}=${String(value ?? "")}`).join(" · ");
}

export default function JobDetailPanel({ job }: JobDetailPanelProps) {
  return (
    <article className="panel-card">
      <div className="panel-card__header">
        <div>
          <p className="eyebrow">Job detail</p>
          <h3>Run {job.run_id}</h3>
        </div>
        <span className={`chip ${job.status === "aborted" || job.status === "partial" ? "chip--warning" : ""}`}>
          {job.status}
        </span>
      </div>

      <div className="detail-grid">
        <label className="field field--readonly">
          <span>Type</span>
          <input value={job.type} readOnly aria-readonly="true" />
        </label>
        <label className="field field--readonly">
          <span>Actor</span>
          <input value={job.actor.username} readOnly aria-readonly="true" />
        </label>
        <label className="field field--readonly">
          <span>Created at</span>
          <input value={job.created_at} readOnly aria-readonly="true" />
        </label>
        <label className="field field--readonly">
          <span>Updated at</span>
          <input value={job.updated_at} readOnly aria-readonly="true" />
        </label>
        <label className="field field--readonly">
          <span>Started at</span>
          <input value={job.started_at || "n/a"} readOnly aria-readonly="true" />
        </label>
        <label className="field field--readonly">
          <span>Finished at</span>
          <input value={job.finished_at || "n/a"} readOnly aria-readonly="true" />
        </label>
      </div>

      <div className="subsection-grid">
        <div>
          <h4>Input</h4>
          <table className="data-table">
            <thead>
              <tr>
                <th>Field</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(job.input).length > 0 ? (
                Object.entries(job.input).map(([key, value]) => (
                  <tr key={key}>
                    <td>{key}</td>
                    <td>{String(value ?? "") || "n/a"}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={2}>Safe input metadata відсутні.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div>
          <h4>Summary</h4>
          <p className="helper-text">{renderSummary(job.summary)}</p>
          <table className="data-table">
            <thead>
              <tr>
                <th>Field</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(job.summary).length > 0 ? (
                Object.entries(job.summary).map(([key, value]) => (
                  <tr key={key}>
                    <td>{key}</td>
                    <td>{String(value ?? "") || "n/a"}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={2}>Summary ще відсутній.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <h4>Events</h4>
      <table className="data-table">
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Level</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody>
          {job.events.length > 0 ? (
            job.events.map((event) => (
              <tr key={`${event.timestamp}-${event.message}`}>
                <td>{event.timestamp}</td>
                <td>{event.level}</td>
                <td>{event.message}</td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan={3}>Safe events відсутні.</td>
            </tr>
          )}
        </tbody>
      </table>

      {job.messages.length > 0 ? (
        <div className="message-stack">
          <h4>Messages</h4>
          <ul className="bullet-list">
            {job.messages.map((message) => (
              <li key={message}>{message}</li>
            ))}
          </ul>
        </div>
      ) : null}

      <h4>Artifacts</h4>
      <table className="data-table">
        <thead>
          <tr>
            <th>Kind</th>
            <th>Label</th>
            <th>Local path</th>
            <th>Bucket</th>
            <th>Object key</th>
            <th>Download</th>
          </tr>
        </thead>
        <tbody>
          {job.artifacts.length > 0 ? (
            job.artifacts.map((artifact) => (
              <tr key={`${artifact.kind}-${artifact.label}-${artifact.local_path ?? "none"}`}>
                <td>{artifact.kind}</td>
                <td>{artifact.label}</td>
                <td>{artifact.local_path || "n/a"}</td>
                <td>{artifact.bucket || "n/a"}</td>
                <td>{artifact.object_key || "n/a"}</td>
                <td>
                  {artifact.download_url ? (
                    <a href={artifact.download_url} target="_blank" rel="noreferrer">
                      Download artifact
                    </a>
                  ) : (
                    <span>Download endpoint ще не доданий</span>
                  )}
                </td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan={6}>Artifact refs ще відсутні.</td>
            </tr>
          )}
        </tbody>
      </table>
    </article>
  );
}
