import { startTransition, useState } from "react";

import { generateAttestazione, searchAttestazioni } from "../api/attestazioni";
import { ApiError } from "../api/client";
import type {
  ApiScalar,
  AttestazioneClientInfo,
  AttestazioneGenerateRequest,
  AttestazioneGenerateResponse,
  AttestazioneGenerateRunOnly,
  AttestazioneImmobileIdentity,
  AttestazioneSearchImmobile,
  AttestazioneSearchResponse,
  ClientUpdates,
} from "../types/api";

export type AttestazioneEditableFormState = {
  immobile_comune: string;
  immobile_via: string;
  immobile_civico: string;
  immobile_piano: string;
  immobile_interno: string;
  energy_class: string;
  arredato: string;
  istat: string;
  ignore_surcharges: string;
  contract_kind: string;
};

export type AttestazioneRunOnlyFormState = AttestazioneGenerateRunOnly;

export type AttestazioneImmobileFormState = {
  index: number;
  selected: boolean;
  identity: {
    foglio: string;
    numero: string;
    sub: string;
  };
  visura: {
    rendita: string;
    superficie_totale: string;
    categoria: string;
    visura_comune: string;
    visura_via: string;
    visura_civico: string;
  };
  editable: AttestazioneEditableFormState;
  runOnly: AttestazioneRunOnlyFormState;
  elements: Record<string, string>;
};

const EMPTY_CLIENT_UPDATES: ClientUpdates = {
  locatore_comune_res: "",
  locatore_via: "",
  locatore_civico: "",
};

function toStringValue(value: ApiScalar): string {
  if (typeof value === "string") {
    return value;
  }
  return String(value);
}

function normalizeLocatoreCf(value: string): string {
  return value.trim().toUpperCase();
}

function toIgnoreSurchargesState(value: ApiScalar): string {
  if (value === true) {
    return "true";
  }
  if (value === false) {
    return "false";
  }
  return toStringValue(value);
}

function toGenerateIgnoreSurchargesValue(value: string): string | boolean {
  const normalized = value.trim().toLowerCase();
  if (normalized === "true") {
    return true;
  }
  if (normalized === "false") {
    return false;
  }
  return value;
}

function toIdentityState(identity: AttestazioneImmobileIdentity) {
  return {
    foglio: toStringValue(identity.foglio),
    numero: toStringValue(identity.numero),
    sub: toStringValue(identity.sub),
  };
}

function toClientUpdatesState(client: AttestazioneClientInfo): ClientUpdates {
  return {
    locatore_comune_res: client.locatore_comune_res,
    locatore_via: client.locatore_via,
    locatore_civico: client.locatore_civico,
  };
}

function toImmobileState(immobile: AttestazioneSearchImmobile): AttestazioneImmobileFormState {
  return {
    index: immobile.index,
    selected: immobile.enabled,
    identity: toIdentityState(immobile.identity),
    visura: {
      rendita: toStringValue(immobile.visura.rendita),
      superficie_totale: toStringValue(immobile.visura.superficie_totale),
      categoria: toStringValue(immobile.visura.categoria),
      visura_comune: toStringValue(immobile.visura.visura_comune),
      visura_via: toStringValue(immobile.visura.visura_via),
      visura_civico: toStringValue(immobile.visura.visura_civico),
    },
    editable: {
      immobile_comune: toStringValue(immobile.editable.immobile_comune),
      immobile_via: toStringValue(immobile.editable.immobile_via),
      immobile_civico: toStringValue(immobile.editable.immobile_civico),
      immobile_piano: toStringValue(immobile.editable.immobile_piano),
      immobile_interno: toStringValue(immobile.editable.immobile_interno),
      energy_class: toStringValue(immobile.editable.energy_class),
      arredato: toStringValue(immobile.editable.arredato),
      istat: toStringValue(immobile.editable.istat),
      ignore_surcharges: toIgnoreSurchargesState(immobile.editable.ignore_surcharges),
      contract_kind: toStringValue(immobile.editable.contract_kind),
    },
    runOnly: {
      conduttore_nome: toStringValue(immobile.run_only.conduttore_nome),
      conduttore_cf: toStringValue(immobile.run_only.conduttore_cf),
      conduttore_comune: toStringValue(immobile.run_only.conduttore_comune),
      conduttore_via: toStringValue(immobile.run_only.conduttore_via),
      contratto_data: toStringValue(immobile.run_only.contratto_data),
      decorrenza_data: toStringValue(immobile.run_only.decorrenza_data),
      registrazione_data: toStringValue(immobile.run_only.registrazione_data),
      registrazione_num: toStringValue(immobile.run_only.registrazione_num),
      agenzia_entrate_sede: toStringValue(immobile.run_only.agenzia_entrate_sede),
      canone_contrattuale_mensile: toStringValue(
        immobile.run_only.canone_contrattuale_mensile,
      ),
      durata_anni: toStringValue(immobile.run_only.durata_anni),
    },
    elements: Object.fromEntries(
      Object.entries(immobile.elements)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, value]) => [key, toStringValue(value)]),
    ),
  };
}

function buildGeneratePayload(
  preparedResult: AttestazioneSearchResponse,
  clientUpdates: ClientUpdates,
  immobili: AttestazioneImmobileFormState[],
): AttestazioneGenerateRequest {
  return {
    locatore_cf: preparedResult.client.locatore_cf,
    prepared_immobili_yaml_path: preparedResult.document.immobili_yaml_path,
    client_updates: {
      locatore_comune_res: clientUpdates.locatore_comune_res,
      locatore_via: clientUpdates.locatore_via,
      locatore_civico: clientUpdates.locatore_civico,
    },
    immobili: immobili.map((immobile) => ({
      index: immobile.index,
      enabled: immobile.selected,
      identity: {
        foglio: immobile.identity.foglio,
        numero: immobile.identity.numero,
        sub: immobile.identity.sub,
      },
      editable: {
        immobile_comune: immobile.editable.immobile_comune,
        immobile_via: immobile.editable.immobile_via,
        immobile_civico: immobile.editable.immobile_civico,
        immobile_piano: immobile.editable.immobile_piano,
        immobile_interno: immobile.editable.immobile_interno,
        energy_class: immobile.editable.energy_class,
        arredato: immobile.editable.arredato,
        istat: immobile.editable.istat,
        ignore_surcharges: toGenerateIgnoreSurchargesValue(
          immobile.editable.ignore_surcharges,
        ),
        contract_kind: immobile.editable.contract_kind,
      },
      run_only: {
        conduttore_nome: immobile.runOnly.conduttore_nome,
        conduttore_cf: immobile.runOnly.conduttore_cf,
        conduttore_comune: immobile.runOnly.conduttore_comune,
        conduttore_via: immobile.runOnly.conduttore_via,
        contratto_data: immobile.runOnly.contratto_data,
        decorrenza_data: immobile.runOnly.decorrenza_data,
        registrazione_data: immobile.runOnly.registrazione_data,
        registrazione_num: immobile.runOnly.registrazione_num,
        agenzia_entrate_sede: immobile.runOnly.agenzia_entrate_sede,
        canone_contrattuale_mensile: immobile.runOnly.canone_contrattuale_mensile,
        durata_anni: immobile.runOnly.durata_anni,
      },
      elements: { ...immobile.elements },
    })),
  };
}

function formatSearchError(error: unknown): string {
  if (error instanceof ApiError) {
    switch (error.status) {
      case 401:
        return "Веб-сесія завершилась. Увійдіть знову перед підготовкою даних.";
      case 422:
        return "Перевірте codice fiscale locatore і повторіть пошук.";
      case 400:
        return "Backend відхилив запит на підготовку даних для цього клієнта.";
      case 404:
        return "Підготовлені immobili не знайдено. Повторіть search/prepare.";
      case 503:
        return "Підготовка тимчасово недоступна. Protected import/runtime path зараз не відповідає.";
      default:
        return "Не вдалося підготувати дані клієнта.";
    }
  }

  return "Не вдалося підготувати дані клієнта.";
}

function formatGenerateError(error: unknown): string {
  if (error instanceof ApiError) {
    switch (error.status) {
      case 401:
        return "Веб-сесія завершилась. Увійдіть знову перед генерацією.";
      case 422:
        return "Generation payload не пройшов валідацію. Перевірте поля і спробуйте ще раз.";
      case 400:
        return "Prepared data для генерації невалідні або неповні.";
      case 404:
        return "Prepared YAML більше не знайдено. Повторіть search/prepare.";
      case 409:
        return "Prepared data вже не збігаються з поточним контекстом. Повторіть search/prepare.";
      case 500:
      case 503:
        return "Generation runtime тимчасово недоступний. Спробуйте ще раз пізніше.";
      default:
        return "Не вдалося згенерувати Attestazione.";
    }
  }

  return "Не вдалося згенерувати Attestazione.";
}

export function useAttestazioneFlow() {
  const [locatoreCf, setLocatoreCf] = useState("");
  const [forceUpdateVisura, setForceUpdateVisura] = useState(false);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [preparedResult, setPreparedResult] = useState<AttestazioneSearchResponse | null>(null);
  const [clientUpdates, setClientUpdates] = useState<ClientUpdates>(EMPTY_CLIENT_UPDATES);
  const [immobili, setImmobiles] = useState<AttestazioneImmobileFormState[]>([]);
  const [generationLoading, setGenerationLoading] = useState(false);
  const [generationError, setGenerationError] = useState<string | null>(null);
  const [generationResult, setGenerationResult] = useState<AttestazioneGenerateResponse | null>(
    null,
  );

  const canSearch = normalizeLocatoreCf(locatoreCf).length > 0 && !searchLoading && !generationLoading;
  const canGenerate =
    !searchLoading &&
    !generationLoading &&
    preparedResult !== null &&
    preparedResult.document.immobili_yaml_path.trim().length > 0 &&
    immobili.some((immobile) => immobile.selected);

  const submitSearch = async () => {
    const normalizedCf = normalizeLocatoreCf(locatoreCf);
    if (!normalizedCf) {
      setSearchError("Вкажіть codice fiscale locatore.");
      return;
    }

    setSearchLoading(true);
    setSearchError(null);
    setGenerationError(null);
    startTransition(() => {
      setPreparedResult(null);
      setClientUpdates(EMPTY_CLIENT_UPDATES);
      setImmobiles([]);
      setGenerationResult(null);
    });

    try {
      const response = await searchAttestazioni({
        locatore_cf: normalizedCf,
        force_update_visura: forceUpdateVisura,
      });

      startTransition(() => {
        setLocatoreCf(normalizedCf);
        setPreparedResult(response);
        setClientUpdates(toClientUpdatesState(response.client));
        setImmobiles(response.immobili.map(toImmobileState));
      });
    } catch (error) {
      setSearchError(formatSearchError(error));
    } finally {
      setSearchLoading(false);
    }
  };

  const submitGeneration = async () => {
    if (!preparedResult) {
      setGenerationError("Спочатку виконайте пошук / підготовку даних.");
      return;
    }

    if (!immobili.some((immobile) => immobile.selected)) {
      setGenerationError("Оберіть хоча б один immobile для генерації.");
      return;
    }

    setGenerationLoading(true);
    setGenerationError(null);
    setGenerationResult(null);

    try {
      const response = await generateAttestazione(
        buildGeneratePayload(preparedResult, clientUpdates, immobili),
      );
      startTransition(() => {
        setGenerationResult(response);
      });
    } catch (error) {
      setGenerationError(formatGenerateError(error));
    } finally {
      setGenerationLoading(false);
    }
  };

  const updateClientField = (field: keyof ClientUpdates, value: string) => {
    setClientUpdates((current) => ({
      ...current,
      [field]: value,
    }));
  };

  const updateSelected = (index: number, selected: boolean) => {
    setImmobiles((current) =>
      current.map((immobile) =>
        immobile.index === index ? { ...immobile, selected } : immobile,
      ),
    );
  };

  const updateEditableField = (
    index: number,
    field: keyof AttestazioneEditableFormState,
    value: string,
  ) => {
    setImmobiles((current) =>
      current.map((immobile) =>
        immobile.index === index
          ? {
              ...immobile,
              editable: {
                ...immobile.editable,
                [field]: value,
              },
            }
          : immobile,
      ),
    );
  };

  const updateRunOnlyField = (
    index: number,
    field: keyof AttestazioneRunOnlyFormState,
    value: string,
  ) => {
    setImmobiles((current) =>
      current.map((immobile) =>
        immobile.index === index
          ? {
              ...immobile,
              runOnly: {
                ...immobile.runOnly,
                [field]: value,
              },
            }
          : immobile,
      ),
    );
  };

  const updateElementField = (index: number, elementKey: string, value: string) => {
    setImmobiles((current) =>
      current.map((immobile) =>
        immobile.index === index
          ? {
              ...immobile,
              elements: {
                ...immobile.elements,
                [elementKey]: value,
              },
            }
          : immobile,
      ),
    );
  };

  return {
    locatoreCf,
    setLocatoreCf,
    forceUpdateVisura,
    setForceUpdateVisura,
    searchLoading,
    searchError,
    preparedResult,
    clientUpdates,
    immobili,
    generationLoading,
    generationError,
    generationResult,
    canSearch,
    canGenerate,
    submitSearch,
    submitGeneration,
    updateClientField,
    updateSelected,
    updateEditableField,
    updateRunOnlyField,
    updateElementField,
  };
}
