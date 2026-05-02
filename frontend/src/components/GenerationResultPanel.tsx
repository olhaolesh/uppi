import type { AttestazioneGenerateResponse } from "../types/api";

type GenerationResultPanelProps = {
  result: AttestazioneGenerateResponse;
};

export default function GenerationResultPanel({ result }: GenerationResultPanelProps) {
  return (
    <article className="panel-card">
      <div className="panel-card__header">
        <div>
          <p className="eyebrow">Generation result</p>
          <h3>Run {result.run_id}</h3>
        </div>
        <span className="chip">{result.status}</span>
      </div>

      <div className="stats-grid">
        <div>
          <span>Requested</span>
          <strong>{result.summary.requested_count}</strong>
        </div>
        <div>
          <span>Generated</span>
          <strong>{result.summary.generated_count}</strong>
        </div>
        <div>
          <span>Failed</span>
          <strong>{result.summary.failed_count}</strong>
        </div>
        <div>
          <span>Locatore CF</span>
          <strong>{result.locatore_cf}</strong>
        </div>
      </div>

      <div className="detail-grid detail-grid--single">
        <label className="field field--readonly">
          <span>Prepared YAML path</span>
          <input value={result.input.prepared_immobili_yaml_path} readOnly aria-readonly="true" />
        </label>
        <label className="field field--readonly">
          <span>Generation YAML path</span>
          <input value={result.input.generation_immobili_yaml_path} readOnly aria-readonly="true" />
        </label>
      </div>

      <h4>Artifacts</h4>
      <table className="data-table">
        <thead>
          <tr>
            <th>Index</th>
            <th>Identity</th>
            <th>Kind</th>
            <th>Local path</th>
            <th>Bucket</th>
            <th>Object key</th>
            <th>Download</th>
          </tr>
        </thead>
        <tbody>
          {result.artifacts.map((artifact) => (
            <tr key={`${artifact.index}-${artifact.kind}`}>
              <td>{artifact.index}</td>
              <td>
                F{String(artifact.identity.foglio)} / N{String(artifact.identity.numero)} / S
                {String(artifact.identity.sub)}
              </td>
              <td>{artifact.kind}</td>
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
          ))}
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
          Без додаткових backend messages. Artifact refs показані як технічний результат
          synchronous MVP flow.
        </p>
      )}
    </article>
  );
}
