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
		tech_pk integer NOT NULL
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
		
	-- seq_node_pk
	CREATE SEQUENCE live_network.seq_node_pk
		INCREMENT 1
		START 302
		MINVALUE 1
		MAXVALUE 9223372036854775807
		CACHE 1;

	ALTER SEQUENCE live_network.seq_node_pk
		OWNER TO bodastage;

	COMMENT ON SEQUENCE live_network.seq_node_pk
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




	
EOSQL