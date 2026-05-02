import { useEffect, useState } from "react";

import { ApiError } from "../api/client";
import { getJob, listJobs } from "../api/jobs";
import JobDetailPanel from "../components/JobDetailPanel";
import StatusPanel from "../components/StatusPanel";
import type { JobDetail, JobStatus, JobSummaryItem, JobType } from "../types/api";

function mapJobsError(error: unknown): string {
  if (error instanceof ApiError) {
    switch (error.status) {
      case 401:
        return "Веб-сесія завершилась. Увійдіть знову, щоб переглянути jobs.";
      case 404:
        return "Потрібний job більше не знайдено.";
      default:
        return "Не вдалося завантажити jobs.";
    }
  }

  if (error instanceof Error) {
    return error.message || "Не вдалося завантажити jobs.";
  }

  return "Не вдалося завантажити jobs.";
}

function renderSummaryPreview(summary: JobSummaryItem["summary"]): string {
  const entries = Object.entries(summary);
  if (entries.length === 0) {
    return "n/a";
  }
  return entries
    .slice(0, 3)
    .map(([key, value]) => `${key}=${String(value ?? "")}`)
    .join(" · ");
}

export default function JobStatusPage() {
  const [typeFilter, setTypeFilter] = useState<JobType | "">("");
  const [statusFilter, setStatusFilter] = useState<JobStatus | "">("");
  const [jobs, setJobs] = useState<JobSummaryItem[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedJob, setSelectedJob] = useState<JobDetail | null>(null);
  const [listLoading, setListLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [listError, setListError] = useState<string | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);

  async function loadJobDetail(runId: string) {
    setDetailLoading(true);
    setDetailError(null);

    try {
      const detail = await getJob(runId);
      setSelectedRunId(runId);
      setSelectedJob(detail);
    } catch (caughtError) {
      setDetailError(mapJobsError(caughtError));
      setSelectedJob(null);
    } finally {
      setDetailLoading(false);
    }
  }

  async function loadJobs() {
    setListLoading(true);
    setListError(null);

    try {
      const response = await listJobs({
        type: typeFilter,
        status: statusFilter,
        limit: 50,
      });
      setJobs(response.jobs);

      if (response.jobs.length === 0) {
        setSelectedRunId(null);
        setSelectedJob(null);
        return;
      }

      const nextRunId =
        selectedRunId && response.jobs.some((job) => job.run_id === selectedRunId)
          ? selectedRunId
          : response.jobs[0].run_id;
      await loadJobDetail(nextRunId);
    } catch (caughtError) {
      setListError(mapJobsError(caughtError));
      setJobs([]);
      setSelectedRunId(null);
      setSelectedJob(null);
    } finally {
      setListLoading(false);
    }
  }

  useEffect(() => {
    void loadJobs();
    // `selectedRunId` is intentionally excluded to avoid refetch loops while selecting rows.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [typeFilter, statusFilter]);

  return (
    <div className="page-stack">
      <section className="hero-card">
        <div>
          <p className="eyebrow">Екран 3</p>
          <h2>Статус / Логи / Артефакти</h2>
        </div>
        <p>
          Екран читає lightweight web job registry через{" "}
          <code>GET /jobs</code> і <code>GET /jobs/{"{run_id}"}</code>. Це не async queue і не
          raw log tail.
        </p>
      </section>

      <section className="panel-grid panel-grid--two">
        <article className="panel-card">
          <div className="panel-card__header">
            <div>
              <h3>Jobs</h3>
              <p className="helper-text">
                Показуються тільки safe metadata, events, messages і artifact refs.
              </p>
            </div>
            <button className="ghost-button" type="button" onClick={() => void loadJobs()}>
              Refresh
            </button>
          </div>

          <div className="detail-grid">
            <label className="field">
              <span>Type filter</span>
              <select value={typeFilter} onChange={(event) => setTypeFilter(event.target.value as JobType | "")}>
                <option value="">Усі типи</option>
                <option value="attestazioni_search">attestazioni_search</option>
                <option value="attestazioni_generate">attestazioni_generate</option>
                <option value="clients_bulk_import">clients_bulk_import</option>
              </select>
            </label>
            <label className="field">
              <span>Status filter</span>
              <select
                value={statusFilter}
                onChange={(event) => setStatusFilter(event.target.value as JobStatus | "")}
              >
                <option value="">Усі статуси</option>
                <option value="running">running</option>
                <option value="completed">completed</option>
                <option value="failed">failed</option>
                <option value="aborted">aborted</option>
                <option value="partial">partial</option>
              </select>
            </label>
          </div>

          {listError ? (
            <div className="inline-alert" role="alert">
              {listError}
            </div>
          ) : null}

          {listLoading ? (
            <p className="helper-text">Завантаження jobs...</p>
          ) : jobs.length === 0 ? (
            <p className="helper-text">Жодного web job ще немає.</p>
          ) : (
            <table className="data-table">
              <thead>
                <tr>
                  <th>run_id</th>
                  <th>type</th>
                  <th>status</th>
                  <th>created_at</th>
                  <th>updated_at</th>
                  <th>summary</th>
                  <th>artifacts</th>
                  <th>Open</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr key={job.run_id}>
                    <td>{job.run_id}</td>
                    <td>{job.type}</td>
                    <td>{job.status}</td>
                    <td>{job.created_at}</td>
                    <td>{job.updated_at}</td>
                    <td>{renderSummaryPreview(job.summary)}</td>
                    <td>{job.artifact_count}</td>
                    <td>
                      <button
                        className="ghost-button"
                        type="button"
                        onClick={() => void loadJobDetail(job.run_id)}
                        disabled={detailLoading && selectedRunId === job.run_id}
                      >
                        {selectedRunId === job.run_id ? `Відкрито ${job.run_id}` : `Відкрити job ${job.run_id}`}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </article>

        <StatusPanel title="Registry model" tone="info">
          <ul className="bullet-list">
            <li>Registry зберігає lightweight records у workspace-local JSON file.</li>
            <li>Raw logs, traceback, secrets, cookies, `state.json` і raw CSV content не exposed.</li>
            <li>Download endpoint для artifacts поки не доданий.</li>
          </ul>
        </StatusPanel>
      </section>

      {detailError ? (
        <div className="inline-alert" role="alert">
          {detailError}
        </div>
      ) : null}

      {detailLoading ? (
        <section className="panel-card">
          <h3>Job detail</h3>
          <p className="helper-text">Завантаження job detail...</p>
        </section>
      ) : selectedJob ? (
        <JobDetailPanel job={selectedJob} />
      ) : (
        <StatusPanel title="Job detail pending">
          <p>
            Виберіть job зі списку. Тут з’являться full metadata, safe events, messages і artifact refs.
          </p>
        </StatusPanel>
      )}
    </div>
  );
}
