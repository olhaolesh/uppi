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

export type ClientUpdates = {
  locatore_comune_res?: string;
  locatore_via?: string;
  locatore_civico?: string;
};

export type ImmobileEditRequest = {
  index: number;
  enabled: boolean;
  identity: {
    foglio: string;
    numero: string;
    sub: string;
  };
  editable: Record<string, string | boolean>;
  run_only: Record<string, string>;
  elements: Record<string, string>;
};

export type AttestazioneGenerateRequest = {
  locatore_cf: string;
  prepared_immobili_yaml_path: string;
  client_updates: ClientUpdates;
  immobili: ImmobileEditRequest[];
};

export type BulkImportRequest = {
  csv_content: string;
  force_update_visura?: boolean;
  fail_fast?: boolean;
};
