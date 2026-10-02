-- Example schema for pg-bisync: every synced table has a single-column primary key and an
-- updated_at column. Content that other rows refer to is soft deleted (deleted_at).
CREATE TABLE owner (
  id text PRIMARY KEY,
  name text NOT NULL,
  email text NOT NULL UNIQUE,
  enabled boolean NOT NULL DEFAULT true,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- login state lives in its own table: written on the user's home node, never together with the
-- admin columns of owner (a row must not have two writers on different nodes)
CREATE TABLE owner_login (
  owner_id text PRIMARY KEY REFERENCES owner (id) ON DELETE CASCADE,
  otp text NOT NULL DEFAULT '',
  failed integer NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE asset (
  id text PRIMARY KEY,
  name text NOT NULL,
  hash text NOT NULL UNIQUE,
  deleted_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE project (
  id text PRIMARY KEY,
  owner_id text NOT NULL REFERENCES owner (id) ON DELETE CASCADE,
  name text NOT NULL,
  notes text NOT NULL DEFAULT '',
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE item (
  id text PRIMARY KEY,
  project_id text NOT NULL REFERENCES project (id) ON DELETE CASCADE,
  -- DEFERRABLE: asset uses unique_policy merge (references move to the winning row)
  asset_id text REFERENCES asset (id) DEFERRABLE,
  pos integer NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE access (
  id text PRIMARY KEY,
  owner_id text NOT NULL REFERENCES owner (id) ON DELETE CASCADE,
  project_id text NOT NULL REFERENCES project (id) ON DELETE CASCADE,
  expires timestamptz NOT NULL DEFAULT '9999-12-31',
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (owner_id, project_id)
);
