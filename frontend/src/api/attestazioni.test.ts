import { afterEach, describe, expect, it, vi } from "vitest";

import { generateAttestazione, searchAttestazioni } from "./attestazioni";

describe("attestazioni api client", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    localStorage.clear();
    sessionStorage.clear();
  });

  it("posts search and generate requests with credentials include", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
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
              immobili_count: 1,
              active_count: 1,
            },
            immobili: [],
            messages: [],
          }),
          {
            status: 200,
            headers: {
              "Content-Type": "application/json",
            },
          },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
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
            artifacts: [],
            messages: [],
          }),
          {
            status: 200,
            headers: {
              "Content-Type": "application/json",
            },
          },
        ),
      );

    vi.stubGlobal("fetch", fetchMock);

    await searchAttestazioni({
      locatore_cf: "RSSMRA80A01H501Z",
      force_update_visura: true,
    });
    await generateAttestazione({
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
          },
        },
      ],
    });

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(String(fetchMock.mock.calls[0][0])).toContain("/attestazioni/search");
    expect(String(fetchMock.mock.calls[1][0])).toContain("/attestazioni/generate");
    expect(fetchMock.mock.calls.every(([, init]) => init?.credentials === "include")).toBe(true);
    expect(JSON.parse(String(fetchMock.mock.calls[0][1]?.body))).toEqual({
      locatore_cf: "RSSMRA80A01H501Z",
      force_update_visura: true,
    });
    expect(JSON.parse(String(fetchMock.mock.calls[1][1]?.body))).toMatchObject({
      locatore_cf: "RSSMRA80A01H501Z",
      prepared_immobili_yaml_path: "clients/web_prepare/RSSMRA80A01H501Z/immobili.yml",
    });
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });
});
