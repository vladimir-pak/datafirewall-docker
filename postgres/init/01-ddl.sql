-- 01-ddl.sql
-- Executed FIRST on a fresh PostgreSQL data directory.
-- Add your DataFirewall CREATE SCHEMA, CREATE TABLE, CREATE INDEX,
-- and other DDL statements below.
-- No example schema is created here: replace these comments with your real DDL.

CREATE SCHEMA IF NOT EXISTS datafirewall;
CREATE SCHEMA IF NOT EXISTS df_meta_uslik;
CREATE SCHEMA IF NOT EXISTS dq_audit;

-- Bootstrap health marker. Leave this in place.
CREATE TABLE IF NOT EXISTS public.dfw_bootstrap (
    stage TEXT PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS datafirewall.dictionaries (
	id text NULL,
	code text NULL,
	"name" text NULL,
	description text NULL,
	category text NULL,
	"type" text NULL,
	validation_rules text NULL,
	is_active bool NULL DEFAULT true,
	created_by text NULL,
	created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
	uuid uuid NOT NULL DEFAULT gen_random_uuid(),
	CONSTRAINT dictionaries_pkey PRIMARY KEY (uuid)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dictionaries_code ON datafirewall.dictionaries USING btree (code);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dictionaries_id ON datafirewall.dictionaries USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dictionaries_uuid ON datafirewall.dictionaries USING btree (uuid);

CREATE TABLE IF NOT EXISTS datafirewall.dictionary_items (
	id text NULL,
	dictionary_id text NULL,
	code text NULL,
	"name" text NULL,
	description text NULL,
	value text NULL,
	sort_order int4 NULL DEFAULT 0,
	is_active bool NULL DEFAULT true,
	parent_id uuid NULL,
	created_by text NULL,
	created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
	uuid uuid NOT NULL DEFAULT gen_random_uuid(),
	dictionary_uuid uuid NOT NULL,
	CONSTRAINT dictionary_items_pkey PRIMARY KEY (uuid),
	CONSTRAINT dictionary_items_dictionary_uuid_fkey FOREIGN KEY (dictionary_uuid) REFERENCES datafirewall.dictionaries(uuid) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dictionary_items_id ON datafirewall.dictionary_items USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dictionary_items_uuid ON datafirewall.dictionary_items USING btree (uuid);

CREATE TABLE IF NOT EXISTS datafirewall.dqchecks (
    id int4 NOT NULL,
    sql_query text NOT NULL,
    error_descr text NULL,
	logic_classif varchar(20),
	uuid uuid,
	control_object text,
	checked_attr text,
    status varchar(20) NOT NULL default 'enabled'
);

CREATE TABLE IF NOT EXISTS df_meta_uslik.v_dfw_datasets (
    dataset_code text NOT NULL,
    control_area text NOT NULL
);

CREATE TABLE datafirewall.datafirewall_journal (
	event_id varchar(255) NULL,
	action_type varchar(255) NOT NULL,
	action_dttm timestamptz NOT NULL,
	kafka_timestamp int8 NOT NULL,
	kafka_partition varchar(50) NOT NULL,
	kafka_offset int8 NOT NULL,
	data_json jsonb NULL,
	created_at timestamptz NOT NULL DEFAULT now()
)
PARTITION BY RANGE (action_dttm);

CREATE TABLE datafirewall.control_areas_in_check (
	check_uuid uuid NULL,
	code text NULL,
	"name" text NULL,
	link_uuid uuid NOT NULL DEFAULT gen_random_uuid()
);

--audit
CREATE TABLE dq_audit.datafirewall_spring_log (
	id serial4 NOT NULL,
	created timestamp NOT NULL,
	log varchar NULL,
	"type" varchar NULL
)
PARTITION BY RANGE (created);

CREATE TABLE dq_audit.datafirewall_flink_log (
	id serial4 NOT NULL,
	created timestamp NOT NULL,
	log varchar NULL,
	"type" varchar NULL
)
PARTITION BY RANGE (created);

CREATE TABLE dq_audit.datafirewall_kafka_log (
	id serial4 NOT NULL,
	created timestamp NOT NULL,
	log varchar NULL,
	"type" varchar NULL
)
PARTITION BY RANGE (created);

CREATE TABLE dq_audit.datafirewall_ignite_log (
	id serial4 NOT NULL,
	created timestamp NOT NULL,
	log varchar NULL,
	"type" varchar NULL
)
PARTITION BY RANGE (created);
