import { useState } from "react";

import StatusPanel from "../components/StatusPanel";

const placeholderImmobili = [
  { index: 1, title: "Foglio 12 / Numero 345 / Sub 7", status: "Prepared placeholder" },
  { index: 2, title: "Foglio 18 / Numero 44 / Sub 2", status: "Disabled until Stage 7" },
];

export default function GenerateAttestazionePage() {
  const [locatoreCf, setLocatoreCf] = useState("");
  const [forceUpdateVisura, setForceUpdateVisura] = useState(false);

  return (
    <div className="page-stack">
      <section className="hero-card">
        <div>
          <p className="eyebrow">Екран 1</p>
          <h2>Згенерувати Attestazione</h2>
        </div>
        <p>
          UI skeleton показує майбутній flow search/prepare/edit/generate, але в цьому
          stage не викликає `/attestazioni/search` або `/attestazioni/generate`.
        </p>
      </section>

      <section className="panel-grid panel-grid--two">
        <article className="panel-card">
          <h3>Пошук і підготовка</h3>
          <div className="form-grid">
            <label className="field">
              <span>Codice fiscale locatore</span>
              <input
                placeholder="RSSMRA80A01H501Z"
                value={locatoreCf}
                onChange={(event) => setLocatoreCf(event.target.value.toUpperCase())}
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

            <button className="primary-button" type="button" disabled>
              Пошук / підготувати дані
            </button>
          </div>
          <p className="helper-text">
            Stage 7 підключить цей блок до current prepare owner path через вже наявний backend
            adapter.
          </p>
        </article>

        <StatusPanel title="Стан інтеграції" tone="warning">
          <p>
            Search, edit і generate залишені mock-only. Skeleton навмисно не запускає
            реальні бізнес-ендпоінти.
          </p>
        </StatusPanel>
      </section>

      <section className="panel-grid panel-grid--two">
        <article className="panel-card">
          <div className="panel-card__header">
            <h3>Placeholder immobili</h3>
            <span className="chip">Prepared list preview</span>
          </div>
          <div className="placeholder-list">
            {placeholderImmobili.map((immobile) => (
              <div className="placeholder-item" key={immobile.index}>
                <div>
                  <strong>Immobile #{immobile.index}</strong>
                  <p>{immobile.title}</p>
                </div>
                <span className="chip chip--muted">{immobile.status}</span>
              </div>
            ))}
          </div>
        </article>

        <article className="panel-card">
          <div className="panel-card__header">
            <h3>Operator edit form</h3>
            <span className="chip">Stage 7 wiring pending</span>
          </div>
          <div className="form-grid form-grid--compact">
            <label className="field">
              <span>Comune immobile</span>
              <input placeholder="PESCARA" />
            </label>
            <label className="field">
              <span>Via immobile</span>
              <input placeholder="VIA ROMA" />
            </label>
            <label className="field">
              <span>Civico</span>
              <input placeholder="10" />
            </label>
            <label className="field">
              <span>Energy class</span>
              <input placeholder="G" />
            </label>
            <label className="field">
              <span>Conduttore nome</span>
              <input placeholder="Mario Rossi" />
            </label>
            <label className="field">
              <span>Contratto data</span>
              <input placeholder="2026-05-02" />
            </label>
            <label className="field">
              <span>Elemento A1</span>
              <input placeholder="X" />
            </label>
            <label className="field">
              <span>Elemento B1</span>
              <input placeholder="" />
            </label>
          </div>
        </article>
      </section>

      <section className="panel-grid panel-grid--two">
        <StatusPanel title="Run-only contract fields">
          <ul className="bullet-list">
            <li>Conduttore, contratto, registrazione і canone показані лише як skeleton-поля.</li>
            <li>Generation YAML builder уже існує в backend, але UI його ще не викликає.</li>
          </ul>
        </StatusPanel>

        <article className="panel-card">
          <h3>Generation action</h3>
          <button className="primary-button" type="button" disabled>
            Згенерувати Attestazione
          </button>
          <p className="helper-text">
            Реальна інтеграція цього екрана йде окремим Stage 7 pass. Тут немає викликів
            `/attestazioni/generate`.
          </p>
        </article>
      </section>
    </div>
  );
}
