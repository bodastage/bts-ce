	#!/bin/bash
#
#Create schemas
#
#
#set -e
psql -v ON_ERROR_STOP=1 --username "bodastage" -d bts  <<-EOSQL
	-- Live network transformed data 
	CREATE SCHEMA live_network
		AUTHORIZATION bodastage;

-- Table: live_network.nodes

-- DROP TABLE live_network.nodes;

CREATE TABLE live_network.nodes
(
    pk integer NOT NULL,
    name character varying(100) COLLATE pg_catalog."default" NOT NULL,
    date_added timestamp without time zone,
    date_modified timestamp without time zone,
    added_by integer,
    modified_by integer,
    type character varying(20) COLLATE pg_catalog."default",
    notes text COLLATE pg_catalog."default",
    vendor_pk integer NOT NULL,
    tech_pk integer NOT NULL,
    CONSTRAINT nodes_pkey PRIMARY KEY (pk)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE live_network.nodes
    OWNER to bodastage;
COMMENT ON TABLE live_network.nodes
    IS 'Network RAN nodes: BSC,MSC,RNC';

COMMENT ON COLUMN live_network.nodes.pk
    IS 'Primary key';

COMMENT ON COLUMN live_network.nodes.name
    IS 'Node name';

COMMENT ON COLUMN live_network.nodes.type
    IS 'Network Node type: MSC,RNC,BSC';

COMMENT ON COLUMN live_network.nodes.vendor_pk
    IS 'Vendor primary key';

COMMENT ON COLUMN live_network.nodes.tech_pk
    IS 'Technology primary key';
	-- ---------------------------------------------------
	
	CREATE TABLE live_network.base_line_values
	(
		pk bigint NOT NULL,
		parameter_pk bigint NOT NULL,
		value character varying(200) COLLATE pg_catalog."default",
		date_added date,
		date_modified date,
		added_by bigint,
		modified_by bigint,
		CONSTRAINT base_line_values_pkey PRIMARY KEY (pk)
	)
	WITH (
		OIDS = FALSE
	)
	TABLESPACE pg_default;

	ALTER TABLE live_network.base_line_values
		OWNER to bodastage;
	COMMENT ON TABLE live_network.base_line_values
		IS 'Base line network values. These are the values with the highest frequency in the network.';
		
	-- ---------------------------------------------------------------------------
	
	-- Table: live_network.cells

	-- DROP TABLE live_network.cells;

	CREATE TABLE live_network.cells
	(
		pk integer NOT NULL,
		name character varying(50) COLLATE pg_catalog."default",
		date_added timestamp without time zone,
		date_modified timestamp without time zone,
		added_by integer,
		modified_by integer,
		site_pk integer,
		notes text COLLATE pg_catalog."default",
		tech_pk bigint,
		vendor_pk bigint,
		CONSTRAINT cells_pkey PRIMARY KEY (pk)
	)
	WITH (
		OIDS = FALSE
	)
	TABLESPACE pg_default;

	ALTER TABLE live_network.cells
		OWNER to bodastage;
		
	-- ---------------------------------------------------------------------------
	-- Table: live_network.relations

	-- DROP TABLE live_network.relations;

	CREATE TABLE live_network.relations
	(
		pk integer NOT NULL,
		svrnode_pk integer NOT NULL,
		svrsite_pk integer NOT NULL,
		svrcell_pk integer NOT NULL,
		nbrnode_pk integer NOT NULL,
		nbrsite_pk integer NOT NULL,
		nbrcell_pk integer NOT NULL,
		svrtech_pk integer NOT NULL,
		nbrtech_pk integer NOT NULL,
		svrvendor_pk integer NOT NULL,
		nbrvendor_pk integer NOT NULL,
		date_added timestamp without time zone NOT NULL,
		date_modified timestamp without time zone NOT NULL,
		added_by integer,
		modified_by integer
	)
	WITH (
		OIDS = FALSE
	)
	TABLESPACE pg_default;

	ALTER TABLE live_network.relations
		OWNER to bodastage;
	COMMENT ON TABLE live_network.relations
		IS 'Nbr relations';

	COMMENT ON COLUMN live_network.relations.svrsite_pk
		IS 'Serving site pk';

	-- ---------------------------------------------------------------------------
	-- Table: live_network.sites

	-- DROP TABLE live_network.sites;

	CREATE TABLE live_network.sites
	(
		pk integer NOT NULL,
		name character varying(100) COLLATE pg_catalog."default",
		date_added timestamp without time zone,
		date_modified timestamp without time zone,
		added_by integer,
		modified_by integer,
		node_pk integer,
		notes text COLLATE pg_catalog."default",
		tech_pk bigint,
		vendor_pk bigint,
		CONSTRAINT sites_pkey PRIMARY KEY (pk)
	)
	WITH (
		OIDS = FALSE
	)
	TABLESPACE pg_default;

	ALTER TABLE live_network.sites
		OWNER to bodastage;
	COMMENT ON TABLE live_network.sites
		IS 'Live network sites';
		
	-- ---------------------------------------------------------------------------
	-- Table: live_network.umts_cells_data

	-- DROP TABLE live_network.umts_cells_data;

	CREATE TABLE live_network.umts_cells_data
	(
		pk bigint NOT NULL,
		added_by integer NOT NULL,
		bch_power integer,
		cell_id integer,
		cell_pk bigint,
		date_added timestamp without time zone,
		lac integer,
		latitude double precision,
		longitude double precision,
		maximum_transmission_power integer,
		modified_by integer NOT NULL,
		name character varying(255) COLLATE pg_catalog."default",
		notes text COLLATE pg_catalog."default",
		primary_sch_power integer,
		rac integer,
		sac integer,
		secondary_sch_power integer,
		site_pk bigint,
		tech_pk bigint,
		uarfcn_dl integer,
		uarfcn_ul integer,
		ura_list character varying(255) COLLATE pg_catalog."default",
		vendor_pk bigint,
		azimuth integer,
		cpich_power integer,
		scrambling_code integer,
		cell_range integer,
		height integer,
		site_sector_carrier character varying(255) COLLATE pg_catalog."default",
		date_modified timestamp without time zone,
		CONSTRAINT umts_cells_data_pkey PRIMARY KEY (pk)
	)
	WITH (
		OIDS = FALSE
	)
	TABLESPACE pg_default;

	ALTER TABLE live_network.umts_cells_data
		OWNER to bodastage;
		
	
	-- --------------------------------------------------------------------------------
-- Table: live_network.gsm_cells_data

-- DROP TABLE live_network.gsm_cells_data;

CREATE TABLE live_network.gsm_cells_data
(
    pk bigint NOT NULL,
    name character varying(200) COLLATE pg_catalog."default",
    cell_pk bigint,
    bcc integer,
    ncc integer,
    bsic character varying(5) COLLATE pg_catalog."default",
    bcch integer,
    lac bigint,
    latitude double precision,
    longitude double precision,
    cgi character varying(200) COLLATE pg_catalog."default",
    azimuth integer,
    height integer,
    mechanical_tilt integer,
    electrical_tilt integer,
    hsn integer,
    hopping_type character varying(100) COLLATE pg_catalog."default",
    tch_carriers character varying(255)[] COLLATE pg_catalog."default",
    CONSTRAINT gsm_cells_data_pkey PRIMARY KEY (pk)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE live_network.gsm_cells_data
    OWNER to bodastage;
	
    -----------------------------------------------------------------------------------
-- Table: live_network.lte_cells_data

-- DROP TABLE live_network.lte_cells_data;

CREATE TABLE live_network.lte_cells_data
(
    pk bigint NOT NULL,
    name character varying(100) COLLATE pg_catalog."default",
    cell_pk bigint,
    pci bigint,
    uarfcn_dl bigint,
    uarfcn_ul bigint,
    tac bigint,
    ecgi character varying(200) COLLATE pg_catalog."default",
    rach_root_sequence character varying(100) COLLATE pg_catalog."default",
    max_tx_power integer,
    latitude double precision,
    longitude double precision,
    ta_mode character varying(50) COLLATE pg_catalog."default",
    ta integer,
    tx_elements integer,
    rx_elements integer,
    azimuth integer,
    height integer,
    mechanical_tilt integer,
    electrical_tilt integer,
    CONSTRAINT lte_cells_data_pkey PRIMARY KEY (pk)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE live_network.lte_cells_data
    OWNER to bodastage;
	-----------------------------------------------------------------------------------
-- Table: live_network.umts_external_cells

-- DROP TABLE live_network.umts_external_cells;

CREATE TABLE live_network.umts_external_cells
(
    pk bigint NOT NULL,
    cell_name character varying(200) COLLATE pg_catalog."default",
    cell_pk bigint,
    rac integer,
    lac integer,
    primary_cpich_power integer,
    secondary_cpich_power integer,
    uarfcn_dl integer,
    uarfcn_ul integer,
    mnc integer,
    mcc integer,
    rnc_id integer,
    ci integer,
    CONSTRAINT umts_external_cells_pkey PRIMARY KEY (pk)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE live_network.umts_external_cells
    OWNER to bodastage;
	-----------------------------------------------------------------------------------
-- Table: live_network.gsm_external_cells

-- DROP TABLE live_network.gsm_external_cells;

CREATE TABLE live_network.gsm_external_cells
(
    pk bigint NOT NULL,
    cell_name character varying(200) COLLATE pg_catalog."default",
    cell_pk bigint,
    node_pk bigint,
    mcc integer,
    mnc integer,
    lac integer,
    bcch integer,
    ncc integer,
    bcc integer,
    ci integer,
    CONSTRAINT gsm_externals_pkey PRIMARY KEY (pk)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE live_network.gsm_external_cells
    OWNER to bodastage;
	-- --------------------------------------------------------------------------------
	CREATE SEQUENCE live_network.seq_base_line_values_pk
		INCREMENT 1
		START 4017
		MINVALUE 1
		MAXVALUE 9223372036854775807
		CACHE 1;

	ALTER SEQUENCE live_network.seq_base_line_values_pk
		OWNER TO bodastage;
		
	-- CElls pk
	CREATE SEQUENCE live_network.seq_cells_pk
		INCREMENT 1
		START 98304
		MINVALUE 1
		MAXVALUE 9223372036854775807
		CACHE 1;

	ALTER SEQUENCE live_network.seq_cells_pk
		OWNER TO bodastage;
		
	-- seq_nodes_pk
	CREATE SEQUENCE live_network.seq_nodes_pk
		INCREMENT 1
		START 302
		MINVALUE 1
		MAXVALUE 9223372036854775807
		CACHE 1;

	ALTER SEQUENCE live_network.seq_nodes_pk
		OWNER TO bodastage;

	COMMENT ON SEQUENCE live_network.seq_nodes_pk
		IS 'Generates the node pk';
		
	-- seq_relations_pk
	CREATE SEQUENCE live_network.seq_relations_pk
		INCREMENT 1
		START 2135666
		MINVALUE 1
		MAXVALUE 9223372036854775807
		CACHE 1;

	ALTER SEQUENCE live_network.seq_relations_pk
		OWNER TO bodastage;
		
			
	-- seq_sites_pk
	CREATE SEQUENCE live_network.seq_sites_pk
		INCREMENT 1
		START 26048
		MINVALUE 1
		MAXVALUE 9223372036854775807
		CACHE 1;

	ALTER SEQUENCE live_network.seq_sites_pk
		OWNER to bodastage;
		
	-- seq_umts_cells_data_pk
	CREATE SEQUENCE live_network.seq_umts_cells_data_pk
		INCREMENT 1
		START 25908
		MINVALUE 1
		MAXVALUE 9223372036854775807
		CACHE 1;

	ALTER SEQUENCE live_network.seq_umts_cells_data_pk
		OWNER TO bodastage;
		
	-- seq_gsm_cells_data_pk
	CREATE SEQUENCE live_network.seq_gsm_cells_data_pk
		INCREMENT 1
		START 1
		MINVALUE 1
		MAXVALUE 9223372036854775807
		CACHE 1;

	ALTER SEQUENCE live_network.seq_gsm_cells_data_pk
		OWNER TO bodastage;
		
    -- seq_lte_cells_data
	CREATE SEQUENCE live_network.seq_lte_cells_data
		INCREMENT 1
		START 1
		MINVALUE 1
		MAXVALUE 9223372036854775807
		CACHE 1;

	ALTER SEQUENCE live_network.seq_lte_cells_data
		OWNER TO bodastage;
	
	-- seq_gsm_external_cells_pk
	CREATE SEQUENCE live_network.seq_gsm_external_cells_pk
		INCREMENT 1
		START 1
		MINVALUE 1
		MAXVALUE 9223372036854775807
		CACHE 1;

	ALTER SEQUENCE live_network.seq_gsm_external_cells_pk
		OWNER TO bodastage;
	

    -- seq_umts_external_cells_pk
	CREATE SEQUENCE live_network.seq_umts_external_cells_pk
		INCREMENT 1
		START 1
		MINVALUE 1
		MAXVALUE 9223372036854775807
		CACHE 1;

	ALTER SEQUENCE live_network.seq_umts_external_cells_pk
		OWNER TO bodastage;
		
	-- ------------------------------------------------------
	-- View: live_network.vw_baseline

	-- DROP VIEW live_network.vw_baseline;

	CREATE OR REPLACE VIEW live_network.vw_baseline AS
	 SELECT t4.name AS vendor,
		t5.name AS technology,
		t3.name AS mo,
		t2.name AS parameter,
		t1.value,
		t1.date_added,
		t1.date_modified
	   FROM live_network.base_line_values t1
		 JOIN vendor_parameters t2 ON t2.pk = t1.parameter_pk
		 JOIN managedobjects t3 ON t3.pk = t2.parent_pk
		 JOIN vendors t4 ON t4.pk = t3.vendor_pk
		 JOIN technologies t5 ON t5.pk = t3.tech_pk;

	ALTER TABLE live_network.vw_baseline
		OWNER TO bodastage;

	-- ------------------------------------------------------
	-- View: live_network.vw_relations

	-- DROP VIEW live_network.vw_relations;

	CREATE OR REPLACE VIEW live_network.vw_relations AS
	 SELECT t1.pk,
		t4.name AS svrnode,
		t3.name AS svrsite,
		t2.name AS svrcell,
		t5.name AS svrvendor,
		t6.name AS svrtechnology,
		t9.name AS nbrnode,
		t8.name AS nbrsite,
		t7.name AS nbrcell,
		t10.name AS nbrvendor,
		t11.name AS nbrtechnology
	   FROM live_network.relations t1
		 JOIN live_network.cells t2 ON t2.pk = t1.svrcell_pk
		 JOIN live_network.sites t3 ON t3.pk = t1.svrsite_pk
		 JOIN live_network.nodes t4 ON t4.pk = t1.svrnode_pk
		 JOIN vendors t5 ON t5.pk = t1.svrvendor_pk
		 JOIN technologies t6 ON t6.pk = t1.svrtech_pk
		 JOIN live_network.cells t7 ON t7.pk = t1.nbrcell_pk
		 JOIN live_network.sites t8 ON t8.pk = t1.nbrsite_pk
		 JOIN live_network.nodes t9 ON t9.pk = t1.nbrnode_pk
		 JOIN vendors t10 ON t10.pk = t1.nbrvendor_pk
		 JOIN technologies t11 ON t11.pk = t1.nbrtech_pk;

	ALTER TABLE live_network.vw_relations
		OWNER TO bodastage;


-- View: live_network.vw_nodes

-- DROP VIEW live_network.vw_nodes;

CREATE OR REPLACE VIEW live_network.vw_nodes AS
 SELECT t1.pk AS id,
    t1.name AS nodename,
    t1.type,
    t2.name AS technology,
    t3.name AS vendor,
    t1.date_added
   FROM live_network.nodes t1
     JOIN technologies t2 ON t2.pk = t1.tech_pk
     JOIN vendors t3 ON t3.pk = t1.vendor_pk;

ALTER TABLE live_network.vw_nodes
    OWNER TO bodastage;

	
-- View: live_network.vw_sites

-- DROP VIEW live_network.vw_sites;

CREATE OR REPLACE VIEW live_network.vw_sites AS
 SELECT t1.pk as id,  t1.name,
    t4.name as node,
    t2.name AS technology,
    t3.name AS vendor,
    t1.date_added
   FROM live_network.sites t1
     LEFT JOIN live_network.nodes t4 ON T4.pk = t1.node_pk
     JOIN technologies t2 ON t2.pk = t1.tech_pk
     JOIN vendors t3 ON t3.pk = t1.vendor_pk;

ALTER TABLE live_network.vw_sites
    OWNER TO bodastage;


-- View: live_network.vw_umts_cell_data

-- DROP VIEW live_network.vw_umts_cell_data;

CREATE OR REPLACE VIEW live_network.vw_umts_cell_data AS
 SELECT t1.name,
    t2.name AS site,
    t4.name AS node,
    t3.name AS vendor,
    t1.bch_power,
    t1.cell_id AS ci,
    t1.lac,
    t1.latitude,
    t1.longitude,
    t1.maximum_transmission_power AS maxtx_power,
    t1.primary_sch_power,
    t1.rac,
    t1.sac,
    t1.secondary_sch_power,
    t1.uarfcn_dl,
    t1.uarfcn_ul,
    t1.ura_list,
    t1.azimuth,
    t1.cpich_power,
    t1.scrambling_code,
    t1.cell_range,
    t1.height,
    t1.site_sector_carrier
   FROM live_network.umts_cells_data t1
     JOIN live_network.sites t2 ON t2.pk = t1.pk
     JOIN vendors t3 ON t3.pk = t1.vendor_pk
     JOIN live_network.nodes t4 ON t4.pk = t2.node_pk;

ALTER TABLE live_network.vw_umts_cell_data
    OWNER TO bodastage;

-- View: live_network.vw_lte_cells_data

-- DROP VIEW live_network.vw_lte_cells_data;

CREATE OR REPLACE VIEW live_network.vw_lte_cells_data AS
 SELECT lte_cells_data.pk,
    lte_cells_data.name,
    lte_cells_data.cell_pk,
    lte_cells_data.pci,
    lte_cells_data.uarfcn_dl,
    lte_cells_data.uarfcn_ul,
    lte_cells_data.tac,
    lte_cells_data.ecgi,
    lte_cells_data.rach_root_sequence,
    lte_cells_data.max_tx_power,
    lte_cells_data.latitude,
    lte_cells_data.longitude,
    lte_cells_data.ta_mode,
    lte_cells_data.ta,
    lte_cells_data.tx_elements,
    lte_cells_data.rx_elements,
    lte_cells_data.azimuth,
    lte_cells_data.height,
    lte_cells_data.mechanical_tilt,
    lte_cells_data.electrical_tilt
   FROM live_network.lte_cells_data;

ALTER TABLE live_network.vw_lte_cells_data
    OWNER TO bodastage;


-- ----------------------------------------
-- View: live_network.vw_gsm_external_cells

-- DROP VIEW live_network.vw_gsm_external_cells;

CREATE OR REPLACE VIEW live_network.vw_gsm_external_cells AS
 SELECT gsm_external_cells.pk,
    gsm_external_cells.cell_name,
    gsm_external_cells.cell_pk,
    gsm_external_cells.node_pk,
    gsm_external_cells.mcc,
    gsm_external_cells.mnc,
    gsm_external_cells.lac,
    gsm_external_cells.bcch,
    gsm_external_cells.ncc,
    gsm_external_cells.bcc,
    gsm_external_cells.ci
   FROM live_network.gsm_external_cells;

ALTER TABLE live_network.vw_gsm_external_cells
    OWNER TO bodastage;


-- ----------------------------------------
-- View: live_network.vw_sites

-- DROP VIEW live_network.vw_sites;

CREATE OR REPLACE VIEW live_network.vw_sites AS
 SELECT t1.pk AS id,
    t1.name,
    t4.name AS node,
    t2.name AS technology,
    t3.name AS vendor,
    t1.date_added
   FROM live_network.sites t1
     LEFT JOIN live_network.nodes t4 ON t4.pk = t1.node_pk
     JOIN technologies t2 ON t2.pk = t1.tech_pk
     JOIN vendors t3 ON t3.pk = t1.vendor_pk;

ALTER TABLE live_network.vw_sites
    OWNER TO bodastage;


	
EOSQL