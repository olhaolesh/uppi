import GenerationResultPanel from "../components/GenerationResultPanel";
import ImmobileEditor from "../components/ImmobileEditor";
import StatusPanel from "../components/StatusPanel";
import { useAttestazioneFlow } from "../hooks/useAttestazioneFlow";

export default function GenerateAttestazionePage() {
  const flow = useAttestazioneFlow();

  return (
    <div className="page-stack">
      <section className="hero-card">
        <div>
          <p className="eyebrow">Екран 1</p>
          <h2>Згенерувати Attestazione</h2>
        </div>
        <p>
          Екран тепер реально викликає `POST /attestazioni/search` і
          `POST /attestazioni/generate`, але не дублює prepare/generation orchestration і не
          ходить у SISTER напряму.
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
                value={flow.locatoreCf}
                disabled={flow.searchLoading || flow.generationLoading}
                onChange={(event) => flow.setLocatoreCf(event.target.value.toUpperCase())}
              />
            </label>

            <label className="checkbox-field">
              <input
                type="checkbox"
                checked={flow.forceUpdateVisura}
                disabled={flow.searchLoading || flow.generationLoading}
                onChange={(event) => flow.setForceUpdateVisura(event.target.checked)}
              />
              <span>Force update visura</span>
            </label>

            <button
              className="primary-button"
              type="button"
              disabled={!flow.canSearch}
              onClick={() => void flow.submitSearch()}
            >
              {flow.searchLoading ? "Підготовка..." : "Пошук / підготувати дані"}
            </button>
          </div>
          <p className="helper-text">
            Search/prepare делегується у current `prepare-by-CF` owner path через backend
            adapter. Frontend не вирішує DB hit/miss, import refresh або generation rules.
          </p>
        </article>

        <StatusPanel
          title={flow.preparedResult ? "Prepared result" : "Початковий стан"}
          tone={flow.preparedResult ? "success" : "info"}
        >
          {flow.preparedResult ? (
            <div className="message-stack">
              <p>Source: {flow.preparedResult.source}</p>
              <p>Immobili count: {flow.preparedResult.document.immobili_count}</p>
              <p>Active count: {flow.preparedResult.document.active_count}</p>
              <label className="field field--readonly">
                <span>Prepared YAML path</span>
                <input
                  value={flow.preparedResult.document.immobili_yaml_path}
                  readOnly
                  aria-readonly="true"
                />
              </label>
              {flow.preparedResult.messages.length > 0 ? (
                <ul className="bullet-list">
                  {flow.preparedResult.messages.map((message) => (
                    <li key={message}>{message}</li>
                  ))}
                </ul>
              ) : (
                <p>Prepared document готовий для operator edits і generation.</p>
              )}
            </div>
          ) : (
            <p>
              Введіть codice fiscale, запустіть підготовку і дочекайтесь prepared YAML, після
              чого стануть доступні immobili, editable fields і generate action.
            </p>
          )}
        </StatusPanel>
      </section>

      {flow.searchError ? (
        <div className="inline-alert" role="alert">
          {flow.searchError}
        </div>
      ) : null}

      {flow.generationError ? (
        <div className="inline-alert" role="alert">
          {flow.generationError}
        </div>
      ) : null}

      {!flow.preparedResult ? (
        <section className="panel-card">
          <h3>Prepared client data ще відсутні</h3>
          <p className="helper-text">
            До успішного `POST /attestazioni/search` кнопка генерації лишається disabled, а
            immobili editor не рендериться.
          </p>
        </section>
      ) : (
        <>
          <section className="panel-grid panel-grid--two">
            <article className="panel-card">
              <h3>Client info</h3>
              <div className="detail-grid">
                <label className="field field--readonly">
                  <span>LOCATORE_CF</span>
                  <input value={flow.preparedResult.client.locatore_cf} readOnly aria-readonly="true" />
                </label>
                <label className="field field--readonly">
                  <span>COMUNE</span>
                  <input value={flow.preparedResult.client.comune} readOnly aria-readonly="true" />
                </label>
                <label className="field field--readonly">
                  <span>TIPO_CATASTO</span>
                  <input value={flow.preparedResult.client.tipo_catasto} readOnly aria-readonly="true" />
                </label>
                <label className="field field--readonly">
                  <span>UFFICIO_LABEL</span>
                  <input value={flow.preparedResult.client.ufficio_label} readOnly aria-readonly="true" />
                </label>
              </div>
            </article>

            <article className="panel-card">
              <h3>Client updates</h3>
              <div className="detail-grid">
                <label className="field">
                  <span>Locatore comune res</span>
                  <input
                    value={flow.clientUpdates.locatore_comune_res}
                    disabled={flow.generationLoading}
                    onChange={(event) =>
                      flow.updateClientField("locatore_comune_res", event.target.value)
                    }
                  />
                </label>
                <label className="field">
                  <span>Locatore via</span>
                  <input
                    value={flow.clientUpdates.locatore_via}
                    disabled={flow.generationLoading}
                    onChange={(event) => flow.updateClientField("locatore_via", event.target.value)}
                  />
                </label>
                <label className="field">
                  <span>Locatore civico</span>
                  <input
                    value={flow.clientUpdates.locatore_civico}
                    disabled={flow.generationLoading}
                    onChange={(event) =>
                      flow.updateClientField("locatore_civico", event.target.value)
                    }
                  />
                </label>
              </div>
            </article>
          </section>

          <section className="page-stack">
            {flow.immobili.map((immobile) => (
              <ImmobileEditor
                key={immobile.index}
                immobile={immobile}
                disabled={flow.generationLoading}
                onSelectedChange={flow.updateSelected}
                onEditableChange={flow.updateEditableField}
                onRunOnlyChange={flow.updateRunOnlyField}
                onElementChange={flow.updateElementField}
              />
            ))}
          </section>

        </>
      )}

      <section className="panel-grid panel-grid--two">
        <StatusPanel
          title="Generation action"
          tone={flow.canGenerate ? "success" : "warning"}
        >
          <div className="message-stack">
            <button
              className="primary-button"
              type="button"
              disabled={!flow.canGenerate}
              onClick={() => void flow.submitGeneration()}
            >
              {flow.generationLoading ? "Генерація..." : "Згенерувати Attestazione"}
            </button>
            <p>
              Generation стане доступною тільки після prepared result і хоча б одного selected
              immobile.
            </p>
          </div>
        </StatusPanel>

        {flow.generationResult ? (
          <GenerationResultPanel result={flow.generationResult} />
        ) : (
          <StatusPanel title="Generation result pending">
            <p>
              {flow.preparedResult
                ? "Після успішного `POST /attestazioni/generate` тут з’являться `run_id`, summary, messages і safe artifact refs."
                : "Спочатку підготуйте дані клієнта через `POST /attestazioni/search`, тоді generation endpoint стане доступним із цього екрана."}
            </p>
          </StatusPanel>
        )}
      </section>
    </div>
  );
}
