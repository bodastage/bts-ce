#!/bin/bash
#
#Create schemas
#
#
#set -e
psql -v ON_ERROR_STOP=1 --username "bodastage" -d bts  <<-EOSQL

CREATE SCHEMA airflow;

-- Users table 
-- Table: public.users

-- DROP TABLE public.users;

CREATE TABLE public.users
(
    pk bigint NOT NULL,
    password character varying(60) COLLATE pg_catalog."default",
    username character varying(255) COLLATE pg_catalog."default" NOT NULL,
    enabled smallint,
    token character varying(255) COLLATE pg_catalog."default",
    is_account_non_expired boolean DEFAULT true,
    is_account_non_locked boolean DEFAULT true,
    is_credentials_non_expired boolean DEFAULT true,
    is_enabled boolean DEFAULT true,
    first_name character varying(255) COLLATE pg_catalog."default",
    last_name character varying(255) COLLATE pg_catalog."default",
    other_names character varying(255) COLLATE pg_catalog."default",
    job_title character varying(255) COLLATE pg_catalog."default",
    phone_number character varying(255) COLLATE pg_catalog."default",
    photo text COLLATE pg_catalog."default",
    CONSTRAINT users_pkey PRIMARY KEY (pk),
    CONSTRAINT uk_r43af9ap4edm43mmtq01oddj6 UNIQUE (username)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.users
    OWNER to bodastage;
	
-- Vendors
	-- Table: public.vendors

	-- DROP TABLE public.vendors;

	CREATE TABLE public.vendors
	(
		pk bigint NOT NULL,
		added_by integer NOT NULL,
		date_added timestamp without time zone,
		date_modified timestamp without time zone,
		modified_by integer NOT NULL,
		name character varying(255) COLLATE pg_catalog."default",
		notes text COLLATE pg_catalog."default",
		CONSTRAINT vendors_pkey PRIMARY KEY (pk)
	)
	WITH (
		OIDS = FALSE
	)
	TABLESPACE pg_default;

	ALTER TABLE public.vendors
		OWNER to bodastage;

	-- ----------------------------------
	-- Table: public.vendor_parameters

	-- DROP TABLE public.vendor_parameters;

	CREATE TABLE public.vendor_parameters
	(
		pk bigint NOT NULL,
		name character varying(200) COLLATE pg_catalog."default" NOT NULL,
		notes text COLLATE pg_catalog."default",
		date_added date,
		date_modified date,
		added_by bigint,
		modified_by bigint,
		parent_pk bigint NOT NULL,
		tech_pk bigint NOT NULL,
		vendor_pk bigint NOT NULL,
		CONSTRAINT vendor_parameters_pkey PRIMARY KEY (pk)
	)
	WITH (
		OIDS = FALSE
	)
	TABLESPACE pg_default;

	ALTER TABLE public.vendor_parameters
		OWNER to bodastage;
-- 
	-- Table: public.technologies

	-- DROP TABLE public.technologies;

	CREATE TABLE public.technologies
	(
		pk bigint NOT NULL,
		added_by integer NOT NULL,
		datecreated timestamp without time zone,
		datemodified timestamp without time zone,
		modified_by integer NOT NULL,
		name character varying(255) COLLATE pg_catalog."default",
		notes text COLLATE pg_catalog."default",
		date_added timestamp without time zone,
		date_modified timestamp without time zone,
		CONSTRAINT technologies_pkey PRIMARY KEY (pk)
	)
	WITH (
		OIDS = FALSE
	)
	TABLESPACE pg_default;

	ALTER TABLE public.technologies
		OWNER to bodastage;
		
---------
-- Table: public.managedobjects_schemas

-- DROP TABLE public.managedobjects_schemas;

CREATE TABLE public.managedobjects_schemas
(
    pk bigint NOT NULL,
    added_by integer NOT NULL,
    date_added timestamp without time zone,
    date_modified timestamp without time zone,
    modified_by integer NOT NULL,
    notes text COLLATE pg_catalog."default",
    schema_name character varying(255) COLLATE pg_catalog."default" NOT NULL,
    tech_pk bigint NOT NULL,
    vendor_pk bigint NOT NULL,
    CONSTRAINT managedobjects_schemas_pkey PRIMARY KEY (pk)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.managedobjects_schemas
    OWNER to bodastage;
	
----
-- Table: public.managedobjects

-- DROP TABLE public.managedobjects;

CREATE TABLE public.managedobjects
(
    pk bigint NOT NULL,
    added_by integer NOT NULL,
    date_added timestamp without time zone,
    date_modified timestamp without time zone,
    label character varying(255) COLLATE pg_catalog."default",
    modified_by integer NOT NULL,
    name character varying(255) COLLATE pg_catalog."default" NOT NULL,
    notes text COLLATE pg_catalog."default",
    parent_pk bigint NOT NULL,
    tech_pk bigint NOT NULL,
    vendor_pk bigint NOT NULL,
    CONSTRAINT managedobjects_pkey PRIMARY KEY (pk)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.managedobjects
    OWNER to bodastage;
	
-- 
CREATE SEQUENCE public.seq_vendors_pk
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.seq_vendors_pk
    OWNER TO bodastage;
	
-- -----------------------------
CREATE SEQUENCE public.seq_users
    INCREMENT 1
   START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.seq_users
    OWNER TO bodastage;
	
-- -------------------------------
CREATE SEQUENCE public.seq_vendor_parameters_pk
    INCREMENT 1
   START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.seq_vendor_parameters_pk
    OWNER TO bodastage;
	
-- -------------------------------------
CREATE SEQUENCE public.seq_technologies_pk
    INCREMENT 1
   START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.seq_technologies_pk
    OWNER TO bodastage;
	
-- ------------------------------------------
CREATE SEQUENCE public.seq_managedobjects_schemas_pk
    INCREMENT 1
   START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.seq_managedobjects_schemas_pk
    OWNER TO bodastage;
-- --------------------------------------------
CREATE SEQUENCE public.seq_managedobjects_pk
    INCREMENT 1
   START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.seq_managedobjects_pk
    OWNER TO bodastage;
	
-- --------------------------------------------------

EOSQL