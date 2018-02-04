#!/bin/bash
#
#Create schemas
#
#
#set -e
psql -v ON_ERROR_STOP=1 --username "bodastage" -d bts  <<-EOSQL

CREATE SCHEMA airflow;


CREATE TABLE users (
    pk bigint NOT NULL,
    password character varying(60),
    username character varying(255) NOT NULL,
    enabled smallint,
    token character varying(255),
    is_account_non_expired boolean DEFAULT true,
    is_account_non_locked boolean DEFAULT true,
    is_credentials_non_expired boolean DEFAULT true,
    is_enabled boolean DEFAULT true,
    first_name character varying(255),
    last_name character varying(255),
    other_names character varying(255),
    job_title character varying(255),
    phone_number character varying(255),
    photo text
);


ALTER TABLE users OWNER TO bodastage;

--
-- TOC entry 18038 (class 0 OID 16389)
-- Dependencies: 224
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: bodastage
--

COPY users (pk, password, username, enabled, token, is_account_non_expired, is_account_non_locked, is_credentials_non_expired, is_enabled, first_name, last_name, other_names, job_title, phone_number, photo) FROM stdin;
1	password	user@bts.bodastage.org	1	12345678912345678	t	t	t	t	Bodastage	Solutions	Boda Telecom Suite - CE	RF Engineer	+0000	\N
\.


--
-- TOC entry 17905 (class 2606 OID 16402)
-- Name: users uk_r43af9ap4edm43mmtq01oddj6; Type: CONSTRAINT; Schema: public; Owner: bodastage
--

ALTER TABLE ONLY users
    ADD CONSTRAINT uk_r43af9ap4edm43mmtq01oddj6 UNIQUE (username);


--
-- TOC entry 17907 (class 2606 OID 16400)
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: bodastage
--

ALTER TABLE ONLY users
    ADD CONSTRAINT users_pkey PRIMARY KEY (pk);
-- -----------------------------------------------------------------------------------------------------
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
		supproted boolean default FALSE, 
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
    name character varying(255) COLLATE pg_catalog."default" NOT NULL,
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
	
-- ---------------------------------------------------------------
-- Table: public.settings

-- DROP TABLE public.settings;

CREATE TABLE public.settings
(
    pk integer NOT NULL,
    name character varying COLLATE pg_catalog."default" NOT NULL,
    data_type character varying(200) COLLATE pg_catalog."default" NOT NULL,
    integer_value integer,
    float_value double precision,
    string_value character varying(200) COLLATE pg_catalog."default",
    text_value text COLLATE pg_catalog."default",
    timestamp_value date,
    label character varying(200) COLLATE pg_catalog."default",
    category_id character varying(200) COLLATE pg_catalog."default",
    CONSTRAINT setttings_pkey PRIMARY KEY (pk),
    CONSTRAINT settings_name_unique UNIQUE (name)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.settings
    OWNER to bodastage;
	
-- --------------------------------
-- Table: public.cache

-- DROP TABLE public.cache;

CREATE TABLE public.cache
(
    pk bigint NOT NULL,
    name character varying(200) COLLATE pg_catalog."default",
    data text COLLATE pg_catalog."default",
    date_created date,
    date_modified date,
    modifed_by bigint,
    added_by bigint NOT NULL,
    CONSTRAINT cache_pkey PRIMARY KEY (pk),
    CONSTRAINT unique_cache_name UNIQUE (name)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.cache
    OWNER to bodastage;
-- -------------------------------
CREATE SEQUENCE public.seq_vendors_pk
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.seq_vendors_pk
    OWNER TO bodastage;
	
-- -----------------------------
CREATE SEQUENCE public.seq_users_pk
    INCREMENT 1
   START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.seq_users_pk
    OWNER TO bodastage;
	
ALTER SEQUENCE seq_users_pk RESTART WITH 2;
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
CREATE SEQUENCE public.seq_settings_pk
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.seq_settings_pk
    OWNER TO bodastage;

------------------------------------------------------
CREATE SEQUENCE public.seq_cache_pk
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.seq_cache_pk
    OWNER TO bodastage;
EOSQL