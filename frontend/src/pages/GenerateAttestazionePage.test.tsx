import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError } from "../api/client";
import type {
  AttestazioneGenerateResponse,
  AttestazioneSearchResponse,
} from "../types/api";
import GenerateAttestazionePage from "./GenerateAttestazionePage";

const attestazioniApiMocks = vi.hoisted(() => ({
  searchAttestazioni: vi.fn(),
  generateAttestazione: vi.fn(),
}));

vi.mock("../api/attestazioni", () => ({
  searchAttestazioni: attestazioniApiMocks.searchAttestazioni,
  generateAttestazione: attestazioniApiMocks.generateAttestazione,
}));

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const searchResponse: AttestazioneSearchResponse = {
  status: "prepared",
  source: "db",
  client: {
    locatore_cf: "RSSMRA80A01H501Z",
    comune: "PESCARA",
    tipo_catasto: "F",
    ufficio_label: "PESCARA Territorio",
    locatore_comune_res: "",
    locatore_via: "",
    locatore_civico: "",
  },
  document: {
    immobili_yaml_path: "clients/web_prepare/RSSMRA80A01H501Z/immobili.yml",
    immobili_count: 2,
    active_count: 1,
  },
  immobili: [
    {
      index: 1,
      enabled: true,
      identity: {
        foglio: "12",
        numero: "345",
        sub: "7",
      },
      visura: {
        rendita: "EUR 123.45",
        superficie_totale: 98.7,
        categoria: "A/2",
        visura_comune: "PESCARA",
        visura_via: "VIA ROMA",
        visura_civico: "10",
      },
      editable: {
        immobile_comune: "",
        immobile_via: "",
        immobile_civico: "",
        immobile_piano: "",
        immobile_interno: "",
        energy_class: "",
        arredato: "",
        istat: "",
        ignore_surcharges: false,
        contract_kind: "ordinario",
      },
      run_only: {
        conduttore_nome: "",
        conduttore_cf: "",
        conduttore_comune: "",
        conduttore_via: "",
        contratto_data: "",
        decorrenza_data: "",
        registrazione_data: "",
        registrazione_num: "",
        agenzia_entrate_sede: "",
        canone_contrattuale_mensile: "",
        durata_anni: "",
      },
      elements: {
        a1: "",
        b1: "",
        c1: "",
        d1: "",
      },
    },
    {
      index: 2,
      enabled: false,
      identity: {
        foglio: "18",
        numero: "44",
        sub: "2",
      },
      visura: {
        rendita: "EUR 45.00",
        superficie_totale: 54,
        categoria: "C/2",
        visura_comune: "PESCARA",
        visura_via: "VIA GARIBALDI",
        visura_civico: "20",
      },
      editable: {
        immobile_comune: "",
        immobile_via: "",
        immobile_civico: "",
        immobile_piano: "",
        immobile_interno: "",
        energy_class: "",
        arredato: "",
        istat: "",
        ignore_surcharges: "",
        contract_kind: "",
      },
      run_only: {
        conduttore_nome: "",
        conduttore_cf: "",
        conduttore_comune: "",
        conduttore_via: "",
        contratto_data: "",
        decorrenza_data: "",
        registrazione_data: "",
        registrazione_num: "",
        agenzia_entrate_sede: "",
        canone_contrattuale_mensile: "",
        durata_anni: "",
      },
      elements: {
        a1: "",
      },
    },
  ],
  messages: [],
};

const generationResponse: AttestazioneGenerateResponse = {
  status: "generated",
  run_id: "run-123",
  locatore_cf: "RSSMRA80A01H501Z",
  input: {
    prepared_immobili_yaml_path: "clients/web_prepare/RSSMRA80A01H501Z/immobili.yml",
    generation_immobili_yaml_path:
      "clients/web_generation/RSSMRA80A01H501Z/run-123/immobili.yml",
  },
  summary: {
    requested_count: 1,
    generated_count: 1,
    failed_count: 0,
  },
  artifacts: [
    {
      index: 1,
      identity: {
        foglio: "12",
        numero: "345",
        sub: "7",
      },
      kind: "attestazione_docx",
      local_path: "downloads/RSSMRA80A01H501Z/ATTESTAZIONE_RSSMRA80A01H501Z_81_F12_N345_S7.docx",
      bucket: "attestazioni",
      object_key: "attestazioni/RSSMRA80A01H501Z/81.docx",
      download_url: null,
    },
  ],
  messages: ["Generated synchronously."],
};

describe("GenerateAttestazionePage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    sessionStorage.clear();
  });

  it("renders the initial search form and disabled generate action", () => {
    render(<GenerateAttestazionePage />);

    expect(screen.getByLabelText("Codice fiscale locatore")).toBeInTheDocument();
    expect(screen.getByLabelText("Force update visura")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Пошук / підготувати дані" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Згенерувати Attestazione" })).toBeDisabled();
  });

  it("calls search with normalized payload, shows loading state and renders prepared data", async () => {
    const pendingSearch = deferred<AttestazioneSearchResponse>();
    attestazioniApiMocks.searchAttestazioni.mockReturnValue(pendingSearch.promise);

    render(<GenerateAttestazionePage />);

    fireEvent.change(screen.getByLabelText("Codice fiscale locatore"), {
      target: { value: " rssmra80a01h501z " },
    });
    fireEvent.click(screen.getByRole("button", { name: "Пошук / підготувати дані" }));

    expect(attestazioniApiMocks.searchAttestazioni).toHaveBeenCalledWith({
      locatore_cf: "RSSMRA80A01H501Z",
      force_update_visura: false,
    });
    expect(screen.getByRole("button", { name: "Підготовка..." })).toBeDisabled();

    pendingSearch.resolve(searchResponse);

    expect(await screen.findByDisplayValue("clients/web_prepare/RSSMRA80A01H501Z/immobili.yml")).toBeInTheDocument();
    expect(screen.getByLabelText("COMUNE")).toHaveValue("PESCARA");
    const editor = screen.getByTestId("immobile-editor-1");
    expect(within(editor).getByLabelText("Foglio")).toHaveAttribute("readonly");
    expect(within(editor).getByLabelText("Visura via")).toHaveAttribute("readonly");
    expect(screen.getByRole("button", { name: "Згенерувати Attestazione" })).toBeEnabled();
  });

  it("updates fields, tracks selection state and calls generate with the expected payload", async () => {
    attestazioniApiMocks.searchAttestazioni.mockResolvedValue(searchResponse);
    attestazioniApiMocks.generateAttestazione.mockResolvedValue(generationResponse);

    render(<GenerateAttestazionePage />);

    fireEvent.change(screen.getByLabelText("Codice fiscale locatore"), {
      target: { value: "RSSMRA80A01H501Z" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Пошук / підготувати дані" }));

    await screen.findByTestId("immobile-editor-1");

    fireEvent.change(screen.getByLabelText("Locatore comune res"), {
      target: { value: "PESCARA" },
    });
    fireEvent.change(screen.getByLabelText("Locatore via"), {
      target: { value: "VIA ROMA" },
    });
    fireEvent.change(screen.getByLabelText("Locatore civico"), {
      target: { value: "10" },
    });

    const firstEditor = screen.getByTestId("immobile-editor-1");
    fireEvent.change(within(firstEditor).getByLabelText("Comune immobile"), {
      target: { value: "PESCARA" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Via immobile"), {
      target: { value: "VIA ROMA" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Civico immobile"), {
      target: { value: "10" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Piano"), {
      target: { value: "1" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Interno"), {
      target: { value: "2" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Energy class"), {
      target: { value: "G" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Arredato"), {
      target: { value: "SI" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Conduttore nome"), {
      target: { value: "Mario Rossi" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Conduttore CF"), {
      target: { value: "RSSMRA80A01H501Z" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Conduttore comune"), {
      target: { value: "PESCARA" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Conduttore via"), {
      target: { value: "VIA VERDI 3" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Contratto data"), {
      target: { value: "2026-05-02" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Decorrenza data"), {
      target: { value: "2026-06-01" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Registrazione data"), {
      target: { value: "2026-05-10" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Registrazione num"), {
      target: { value: "12345" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Agenzia Entrate sede"), {
      target: { value: "PESCARA" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Canone mensile"), {
      target: { value: "500" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Durata anni"), {
      target: { value: "4" },
    });
    fireEvent.change(within(firstEditor).getByLabelText("Elemento a1"), {
      target: { value: "X" },
    });

    const generateButton = screen.getByRole("button", { name: "Згенерувати Attestazione" });
    expect(generateButton).toBeEnabled();

    fireEvent.click(within(firstEditor).getByLabelText("Вибрати immobile 1"));
    expect(generateButton).toBeDisabled();
    fireEvent.click(within(firstEditor).getByLabelText("Вибрати immobile 1"));
    expect(generateButton).toBeEnabled();

    fireEvent.click(generateButton);

    await waitFor(() => {
      expect(attestazioniApiMocks.generateAttestazione).toHaveBeenCalledWith({
        locatore_cf: "RSSMRA80A01H501Z",
        prepared_immobili_yaml_path: "clients/web_prepare/RSSMRA80A01H501Z/immobili.yml",
        client_updates: {
          locatore_comune_res: "PESCARA",
          locatore_via: "VIA ROMA",
          locatore_civico: "10",
        },
        immobili: [
          {
            index: 1,
            enabled: true,
            identity: {
              foglio: "12",
              numero: "345",
              sub: "7",
            },
            editable: {
              immobile_comune: "PESCARA",
              immobile_via: "VIA ROMA",
              immobile_civico: "10",
              immobile_piano: "1",
              immobile_interno: "2",
              energy_class: "G",
              arredato: "SI",
              istat: "",
              ignore_surcharges: false,
              contract_kind: "ordinario",
            },
            run_only: {
              conduttore_nome: "Mario Rossi",
              conduttore_cf: "RSSMRA80A01H501Z",
              conduttore_comune: "PESCARA",
              conduttore_via: "VIA VERDI 3",
              contratto_data: "2026-05-02",
              decorrenza_data: "2026-06-01",
              registrazione_data: "2026-05-10",
              registrazione_num: "12345",
              agenzia_entrate_sede: "PESCARA",
              canone_contrattuale_mensile: "500",
              durata_anni: "4",
            },
            elements: {
              a1: "X",
              b1: "",
              c1: "",
              d1: "",
            },
          },
          {
            index: 2,
            enabled: false,
            identity: {
              foglio: "18",
              numero: "44",
              sub: "2",
            },
            editable: {
              immobile_comune: "",
              immobile_via: "",
              immobile_civico: "",
              immobile_piano: "",
              immobile_interno: "",
              energy_class: "",
              arredato: "",
              istat: "",
              ignore_surcharges: "",
              contract_kind: "",
            },
            run_only: {
              conduttore_nome: "",
              conduttore_cf: "",
              conduttore_comune: "",
              conduttore_via: "",
              contratto_data: "",
              decorrenza_data: "",
              registrazione_data: "",
              registrazione_num: "",
              agenzia_entrate_sede: "",
              canone_contrattuale_mensile: "",
              durata_anni: "",
            },
            elements: {
              a1: "",
            },
          },
        ],
      });
    });

    expect(await screen.findByText("Run run-123")).toBeInTheDocument();
    expect(screen.getByText("Generated synchronously.")).toBeInTheDocument();
    expect(screen.getByText("Download endpoint ще не доданий")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Download artifact" })).not.toBeInTheDocument();
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });

  it("shows safe error messages without raw traceback details", async () => {
    attestazioniApiMocks.searchAttestazioni.mockResolvedValue(searchResponse);
    attestazioniApiMocks.generateAttestazione.mockRejectedValue(
      new ApiError(
        "Prepared data no longer matches the current generation context. Run search/prepare again.",
        409,
        {
          detail:
            "Prepared data no longer matches the current generation context. Run search/prepare again.",
        },
      ),
    );

    render(<GenerateAttestazionePage />);

    fireEvent.change(screen.getByLabelText("Codice fiscale locatore"), {
      target: { value: "RSSMRA80A01H501Z" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Пошук / підготувати дані" }));

    await screen.findByTestId("immobile-editor-1");
    fireEvent.click(screen.getByRole("button", { name: "Згенерувати Attestazione" }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Prepared data вже не збігаються з поточним контекстом. Повторіть search/prepare.",
    );
    expect(screen.queryByText(/Traceback/i)).not.toBeInTheDocument();
  });
});
