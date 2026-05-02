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

export type BulkImportInputInfo = {
  clients_csv_path: string;
  force_update_visura: boolean;
  fail_fast: boolean;
};

export type BulkImportSummary = {
  total_rows: number;
  valid_rows: number;
  invalid_rows: number;
  unique_clients: number;
  imported_count: number;
  failed_count: number;
  skipped_count: number;
};

export type BulkImportRowResult = {
  row_number: number;
  locatore_cf: string;
  status: "imported" | "failed" | "skipped_duplicate";
  message: string;
};

export type BulkImportInvalidRow = {
  row_number: number;
  code: string | null;
  message: string;
};

export type BulkImportResponse = {
  status: "completed" | "aborted";
  run_id: string;
  input: BulkImportInputInfo;
  summary: BulkImportSummary;
  results: BulkImportRowResult[];
  invalid_rows: BulkImportInvalidRow[];
  messages: string[];
};

export type JobType =
  | "attestazioni_search"
  | "attestazioni_generate"
  | "clients_bulk_import";

export type JobStatus = "running" | "completed" | "failed" | "aborted" | "partial";

export type JobActor = {
  username: string;
};

export type JobEvent = {
  timestamp: string;
  level: "info" | "warning" | "error";
  message: string;
};

export type JobArtifact = {
  kind: string;
  label: string;
  local_path: string | null;
  bucket: string | null;
  object_key: string | null;
  download_url: string | null;
};

export type JobSummaryMap = Record<string, ApiScalar | null>;

export type JobSummaryItem = {
  run_id: string;
  type: JobType;
  status: JobStatus;
  created_at: string;
  updated_at: string;
  summary: JobSummaryMap;
  artifact_count: number;
  message_count: number;
};

export type JobDetail = {
  run_id: string;
  type: JobType;
  status: JobStatus;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  finished_at: string | null;
  actor: JobActor;
  input: JobSummaryMap;
  summary: JobSummaryMap;
  artifacts: JobArtifact[];
  events: JobEvent[];
  messages: string[];
};

export type ListJobsResponse = {
  jobs: JobSummaryItem[];
};
