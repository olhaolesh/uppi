import type {
  AttestazioneEditableFormState,
  AttestazioneImmobileFormState,
  AttestazioneRunOnlyFormState,
} from "../hooks/useAttestazioneFlow";

const editableFields: Array<{
  key: keyof AttestazioneEditableFormState;
  label: string;
}> = [
  { key: "immobile_comune", label: "Comune immobile" },
  { key: "immobile_via", label: "Via immobile" },
  { key: "immobile_civico", label: "Civico immobile" },
  { key: "immobile_piano", label: "Piano" },
  { key: "immobile_interno", label: "Interno" },
  { key: "energy_class", label: "Energy class" },
  { key: "arredato", label: "Arredato" },
  { key: "istat", label: "ISTAT" },
  { key: "contract_kind", label: "Contract kind" },
];

const runOnlyFields: Array<{
  key: keyof AttestazioneRunOnlyFormState;
  label: string;
}> = [
  { key: "conduttore_nome", label: "Conduttore nome" },
  { key: "conduttore_cf", label: "Conduttore CF" },
  { key: "conduttore_comune", label: "Conduttore comune" },
  { key: "conduttore_via", label: "Conduttore via" },
  { key: "contratto_data", label: "Contratto data" },
  { key: "decorrenza_data", label: "Decorrenza data" },
  { key: "registrazione_data", label: "Registrazione data" },
  { key: "registrazione_num", label: "Registrazione num" },
  { key: "agenzia_entrate_sede", label: "Agenzia Entrate sede" },
  { key: "canone_contrattuale_mensile", label: "Canone mensile" },
  { key: "durata_anni", label: "Durata anni" },
];

type ImmobileEditorProps = {
  immobile: AttestazioneImmobileFormState;
  disabled?: boolean;
  onSelectedChange: (index: number, selected: boolean) => void;
  onEditableChange: (
    index: number,
    field: keyof AttestazioneEditableFormState,
    value: string,
  ) => void;
  onRunOnlyChange: (
    index: number,
    field: keyof AttestazioneRunOnlyFormState,
    value: string,
  ) => void;
  onElementChange: (index: number, elementKey: string, value: string) => void;
};

function ReadonlyField({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <label className="field field--readonly">
      <span>{label}</span>
      <input value={value} readOnly aria-readonly="true" />
    </label>
  );
}

export default function ImmobileEditor({
  immobile,
  disabled = false,
  onSelectedChange,
  onEditableChange,
  onRunOnlyChange,
  onElementChange,
}: ImmobileEditorProps) {
  return (
    <article
      className={immobile.selected ? "panel-card immobile-card" : "panel-card immobile-card immobile-card--muted"}
      data-testid={`immobile-editor-${immobile.index}`}
    >
      <div className="panel-card__header">
        <div>
          <p className="eyebrow">Immobile #{immobile.index}</p>
          <h3>Foglio {immobile.identity.foglio} / Numero {immobile.identity.numero} / Sub {immobile.identity.sub}</h3>
        </div>
        <label className="checkbox-field">
          <input
            type="checkbox"
            checked={immobile.selected}
            disabled={disabled}
            onChange={(event) => onSelectedChange(immobile.index, event.target.checked)}
            aria-label={`Вибрати immobile ${immobile.index}`}
          />
          <span>{immobile.selected ? "Selected" : "Not selected"}</span>
        </label>
      </div>

      <div className="subsection-grid">
        <section>
          <h4>Readonly identity</h4>
          <div className="detail-grid">
            <ReadonlyField label="Foglio" value={immobile.identity.foglio} />
            <ReadonlyField label="Numero" value={immobile.identity.numero} />
            <ReadonlyField label="Sub" value={immobile.identity.sub} />
          </div>
        </section>

        <section>
          <h4>Readonly visura</h4>
          <div className="detail-grid">
            <ReadonlyField label="Rendita" value={immobile.visura.rendita} />
            <ReadonlyField
              label="Superficie totale"
              value={immobile.visura.superficie_totale}
            />
            <ReadonlyField label="Categoria" value={immobile.visura.categoria} />
            <ReadonlyField label="Visura comune" value={immobile.visura.visura_comune} />
            <ReadonlyField label="Visura via" value={immobile.visura.visura_via} />
            <ReadonlyField label="Visura civico" value={immobile.visura.visura_civico} />
          </div>
        </section>
      </div>

      <div className="subsection-grid">
        <section>
          <h4>Editable fields</h4>
          <div className="detail-grid">
            {editableFields.map((field) => (
              <label className="field" key={field.key}>
                <span>{field.label}</span>
                <input
                  value={immobile.editable[field.key]}
                  disabled={disabled}
                  onChange={(event) =>
                    onEditableChange(immobile.index, field.key, event.target.value)
                  }
                />
              </label>
            ))}
            <label className="field">
              <span>Ignore surcharges</span>
              <select
                value={immobile.editable.ignore_surcharges}
                disabled={disabled}
                onChange={(event) =>
                  onEditableChange(immobile.index, "ignore_surcharges", event.target.value)
                }
              >
                <option value="">(порожньо)</option>
                <option value="false">false</option>
                <option value="true">true</option>
                <option value="-">-</option>
              </select>
            </label>
          </div>
        </section>

        <section>
          <h4>Run-only fields</h4>
          <div className="detail-grid">
            {runOnlyFields.map((field) => (
              <label className="field" key={field.key}>
                <span>{field.label}</span>
                <input
                  value={immobile.runOnly[field.key]}
                  disabled={disabled}
                  onChange={(event) =>
                    onRunOnlyChange(immobile.index, field.key, event.target.value)
                  }
                />
              </label>
            ))}
          </div>
        </section>
      </div>

      <section>
        <h4>Elements</h4>
        {Object.keys(immobile.elements).length > 0 ? (
          <div className="detail-grid">
            {Object.entries(immobile.elements).map(([elementKey, value]) => (
              <label className="field" key={elementKey}>
                <span>Elemento {elementKey}</span>
                <input
                  value={value}
                  disabled={disabled}
                  onChange={(event) =>
                    onElementChange(immobile.index, elementKey, event.target.value)
                  }
                />
              </label>
            ))}
          </div>
        ) : (
          <p className="helper-text">Backend не повернув жодного element key для цього immobile.</p>
        )}
      </section>
    </article>
  );
}
