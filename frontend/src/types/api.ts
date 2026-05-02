export type ApiScalar = string | number | boolean;

export type AuthenticatedUser = {
  username: string;
};

export type AuthStatusResponse = {
  authenticated: boolean;
  user?: AuthenticatedUser | null;
};

export type LogoutResponse = {
  authenticated: false;
};

export type LoginRequest = {
  username: string;
  password: string;
  pin: string;
};

export type AttestazioneSearchRequest = {
  locatore_cf: string;
  force_update_visura?: boolean;
};

export type AttestazioneClientInfo = {
  locatore_cf: string;
  comune: string;
  tipo_catasto: string;
  ufficio_label: string;
  locatore_comune_res: string;
  locatore_via: string;
  locatore_civico: string;
};

export type AttestazionePreparedDocumentInfo = {
  immobili_yaml_path: string;
  immobili_count: number;
  active_count: number;
};

export type AttestazioneImmobileIdentity = {
  foglio: ApiScalar;
  numero: ApiScalar;
  sub: ApiScalar;
};

export type AttestazioneImmobileVisura = {
  rendita: ApiScalar;
  superficie_totale: ApiScalar;
  categoria: ApiScalar;
  visura_comune: ApiScalar;
  visura_via: ApiScalar;
  visura_civico: ApiScalar;
};

export type AttestazioneImmobileEditable = {
  immobile_comune: ApiScalar;
  immobile_via: ApiScalar;
  immobile_civico: ApiScalar;
  immobile_piano: ApiScalar;
  immobile_interno: ApiScalar;
  energy_class: ApiScalar;
  arredato: ApiScalar;
  istat: ApiScalar;
  ignore_surcharges: ApiScalar;
  contract_kind: ApiScalar;
};

export type AttestazioneImmobileRunOnly = {
  conduttore_nome: ApiScalar;
  conduttore_cf: ApiScalar;
  conduttore_comune: ApiScalar;
  conduttore_via: ApiScalar;
  contratto_data: ApiScalar;
  decorrenza_data: ApiScalar;
  registrazione_data: ApiScalar;
  registrazione_num: ApiScalar;
  agenzia_entrate_sede: ApiScalar;
  canone_contrattuale_mensile: ApiScalar;
  durata_anni: ApiScalar;
};

export type AttestazioneElementsMap = Record<string, ApiScalar>;

export type AttestazioneSearchImmobile = {
  index: number;
  enabled: boolean;
  identity: AttestazioneImmobileIdentity;
  visura: AttestazioneImmobileVisura;
  editable: AttestazioneImmobileEditable;
  run_only: AttestazioneImmobileRunOnly;
  elements: AttestazioneElementsMap;
};

export type AttestazioneSearchResponse = {
  status: "prepared";
  source: "db" | "sister" | "unknown";
  client: AttestazioneClientInfo;
  document: AttestazionePreparedDocumentInfo;
  immobili: AttestazioneSearchImmobile[];
  messages: string[];
};

export type ClientUpdates = {
  locatore_comune_res: string;
  locatore_via: string;
  locatore_civico: string;
};

export type AttestazioneGenerateEditable = {
  immobile_comune: string;
  immobile_via: string;
  immobile_civico: string;
  immobile_piano: string;
  immobile_interno: string;
  energy_class: string;
  arredato: string;
  istat: string;
  ignore_surcharges: string | boolean;
  contract_kind: string;
};

export type AttestazioneGenerateRunOnly = {
  conduttore_nome: string;
  conduttore_cf: string;
  conduttore_comune: string;
  conduttore_via: string;
  contratto_data: string;
  decorrenza_data: string;
  registrazione_data: string;
  registrazione_num: string;
  agenzia_entrate_sede: string;
  canone_contrattuale_mensile: string;
  durata_anni: string;
};

export type AttestazioneGenerateElementsMap = Record<string, string>;

export type AttestazioneGenerateImmobileRequest = {
  index: number;
  enabled: boolean;
  identity: AttestazioneImmobileIdentity;
  editable: AttestazioneGenerateEditable;
  run_only: AttestazioneGenerateRunOnly;
  elements: AttestazioneGenerateElementsMap;
};

export type AttestazioneGenerateRequest = {
  locatore_cf: string;
  prepared_immobili_yaml_path: string;
  client_updates: ClientUpdates;
  immobili: AttestazioneGenerateImmobileRequest[];
};

export type AttestazioneArtifactRef = {
  index: number;
  identity: AttestazioneImmobileIdentity;
  kind: "attestazione_docx";
  local_path: string | null;
  bucket: string | null;
  object_key: string | null;
  download_url: string | null;
};

export type AttestazioneGenerateResponse = {
  status: "generated";
  run_id: string;
  locatore_cf: string;
  input: {
    prepared_immobili_yaml_path: string;
    generation_immobili_yaml_path: string;
  };
  summary: {
    requested_count: number;
    generated_count: number;
    failed_count: number;
  };
  artifacts: AttestazioneArtifactRef[];
  messages: string[];
};

export type BulkImportRequest = {
  csv_content: string;
  force_update_visura?: boolean;
  fail_fast?: boolean;
};
