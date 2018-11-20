from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import os
import subprocess
import logging


class EricssonCM(object):
    """Process Ericsson configuration management data"""

    def __init__(self):
        self.db_engine = create_engine('postgresql://bodastage:password@database/bts')

    def extract_live_network_rncs(self):
        """Extract Huawei RNCs"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            WITH rncs as (
            SELECT
            DISTINCT
                 "varDateTime" ,
            "SubNetwork_2_id" as name
            FROM ericsson_bulkcm."NodeBFunction" t1
            )
            INSERT INTO live_network.nodes
             (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
             SELECT 
             NEXTVAL('live_network.seq_nodes_pk'),
             "varDateTime" as date_added, 
             "varDateTime" as date_modified, 
             'RNC' as node_type,
             t1."name" as "name" , 
             1 as vendor_pk, -- 1=Ericsson, 2=Huawei, 3-ZTE
             2 as tech_pk , -- 1=gsm, 2-umts,3=lte
             0 as added_by,
             0 as modified_by
            FROM rncs t1
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."name" = t2."name"
            WHERE 
            t2."name" IS NULL
         """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()


    def extract_ericsson_3g_sites(self):
        """Extract Ericsson NodeBs"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.sites
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, node_pk)
            SELECT 
            NEXTVAL('live_network.seq_sites_pk'),
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            0 as added_by,
            0 as modified_by,
            2, -- tech 3 -lte, 2 -umts, 1-gms
            1, -- 1- Ericsson, 2 - Huawei,
            t1."MeContext_id",
            t2.pk -- node primary key
            from ericsson_bulkcm."NodeBFunction" t1
            INNER join live_network.nodes t2 on t2."name" = t1."SubNetwork_2_id" 
                AND t2.vendor_pk = 1 and t2.tech_pk = 2
            LEFT JOIN live_network.sites t3 on t3."name" = t1."MeContext_id" 
               AND t2.vendor_pk = 1 and t2.tech_pk = 2
            WHERE 
            t3."name" IS NULL

        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g_cells(self):
        """Extract Ericsson UTMS Cells"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.cells
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
            SELECT 
            nextval('live_network.seq_cells_pk'),
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            0 as added_by,
            0 as modified_by,
            2, -- tech 3 -lte, 2 -umts, 1-gms
            1, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            t1."userLabel",
            t4.pk -- site primary key
            FROM ericsson_cm_3g."UtranCell" t1
            INNER JOIN ericsson_bulkcm."NodeBFunction" t2 on t2."nodeBFunctionIubLink" = t1."utranCellIubLink"
            INNER JOIN live_network.nodes t3 on t3."name" = t1."MeContext_id" 
                    AND t3.vendor_pk = 1
                    AND t3.tech_pk = 2
            INNER JOIN live_network.sites t4 on t4."name" = t2."MeContext_id"
                AND t4.vendor_pk = 1 
                AND t4.tech_pk = 2
                AND t4.node_pk = t3.pk
            LEFT JOIN live_network.cells t5 on t5."name" = t1."UtranCell_id"
                AND t5.tech_pk = 2
                AND t5.vendor_pk = 1
            WHERE 
            t5."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g_externals_on_2g(self):
        """Extract Ericsson live network 2G externals on 2G """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql  = """
        INSERT INTO live_network.gsm_external_cells
        (pk, name, cell_pk, node_pk, mcc, mnc, lac, bcch, ncc, bcc, ci, rac, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
        t1."CELL_NAME" AS "name",
        t3.pk AS cell_pk,
        t2.pk AS node_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."LAC"::integer AS lac,
        t1."BCCHNO"::integer AS bcch,
        t1."NCC"::integer AS ncc,
        t1."BCC"::integer AS bcc,
        t1."CI"::integer AS ci,
        t1."RAC"::integer AS rac,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        ericsson_cm_2g."EXTERNAL_CELL" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = t1."BSC_NAME"
        LEFT JOIN live_network.cells t3 on t3."name" = t1."CELL_NAME"
        LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."CELL_NAME" 
            AND t4.lac = t1."LAC"::integer
            AND t4.ci = t1."CI"::integer
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g_externals_on_2g(self):
        """Extract live network 3G externals defined on Ericsson BSCs"""

        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql  = """
        INSERT INTO live_network.umts_external_cells
        (pk, name, cell_pk,  mcc, mnc, lac, rncid, ci, psc, uarfcn_dl, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_umts_external_cells_pk') as pk,
        t1."CELL_NAME" AS "name",
        t3.pk AS cell_pk,
        t1."MCC"::integer AS mcc,
        t1."MNC"::integer AS mnc,
        t1."LAC"::integer AS lac,
        t1."RNCID"::integer as rncid,
        t1."CI"::integer as ci,
        t1."SCRCODE"::integer AS psc,
        t1."FDDARFCN"::integer AS dl_uarfcn,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        ericsson_cm_2g."UTRAN_EXTERNAL_CELL" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = t1."BSC_NAME"
        LEFT JOIN live_network.cells t3 on t3."name" = t1."CELL_NAME"
        LEFT JOIN live_network.umts_external_cells t4 on t4."name" = t1."CELL_NAME" 
            AND t4.lac = t1."LAC"::integer
            AND t4.ci = t1."CI"::integer
        WHERE 
        t4.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g_externals_on_2g(self):
        pass

    def extract_live_network_2g_externals_on_3g(self):
        """Extract live network 3G externals defined on Ericsson RNCs"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.gsm_external_cells
        (pk, name, cell_pk, node_pk, mcc, mnc, lac, bcch, ncc, bcc, ci, rac, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
        t1."userLabel" AS "name",
        t3.pk AS cell_pk,
        t2.pk AS node_pk,
        t1."mcc"::integer AS mcc,
        t1."mnc"::integer AS mnc,
        t1."lac"::integer AS lac,
        t1."bcchFrequency"::integer AS bcch,
        t1."ncc"::integer AS ncc,
        t1."bcc"::integer AS bcc,
        t1."cellIdentity"::integer AS ci,
        t1."rac"::integer AS rac,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        ericsson_cm_3g."ExternalGsmCell" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = 'SubNetwork' and t2.vendor_pk = 1 and t2.tech_pk = 2
        LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
        LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."userLabel" 
            AND t4.lac = t1."lac"::integer
            AND t4.ci = t1."cellIdentity"::integer
        WHERE 
        t4.pk IS NULL
        """
        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g_externals_on_3g(self):
        """Extract network externals defined on Ericsson 3G"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.umts_external_cells
        (pk, name, cell_pk,  mcc, mnc, lac, rac, rncid, ci, psc, uarfcn_dl, uarfcn_ul, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_umts_external_cells_pk') as pk,
        t1."userLabel" AS "name",
        t3.pk AS cell_pk,
        t1."mcc"::integer AS mcc,
        t1."mnc"::integer AS mnc,
        t1."lac"::integer AS lac,
        t1."rac"::integer AS rac,
        t1."rncId"::integer as rncid,
        t1."cId"::integer as ci,
        t1."primaryScramblingCode"::integer AS psc,
        t1."uarfcnDl"::integer AS uarfcn_dl,
        t1."uarfcnUl"::integer AS uarfcn_ul,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        ericsson_cm_3g."ExternalUtranCell" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = 'SubNetwork' and t2.vendor_pk = 1 AND tech_pk = 2
        LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
        LEFT JOIN live_network.umts_external_cells t4 on t4."name" = t1."userLabel" 
            AND t4.lac = t1."lac"::integer
        WHERE 
        t4.pk IS NULL
        """
        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g_externals_on_3g(self):
        """Extract network externals defined on Ericsson 3G"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.lte_external_cells
        (pk, name, node_pk, cell_pk, mcc, mnc, pci, dl_earfcn, ci, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
        t1."userLabel" AS "name",
        t5.pk as node_pk,
        t3.pk AS cell_pk,
        null AS mcc,
        null AS mnc,
        (t1."localCellId"::integer + 3 * t1."physicalLayerCellIdGroup"::integer) AS pci,
        t1."earfcndl"::integer AS dl_earfcn_dl,
        t1."localCellId"::integer AS ci,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        ericsson_cm_3g."ExternalEUtranCellFDD" t1
        LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
        LEFT JOIN live_network.lte_external_cells t4 on t4."name" = t1."userLabel" 
            AND t4.ci = t1."localCellId"::integer
        LEFT JOIN live_network.nodes t5 ON t5.name = 'SubNetwork' AND t5.vendor_pk = 1 AND t5.tech_pk = 2
        WHERE 
        t4.pk IS NULL
        """
        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g_externals_4g(self):
        """Extract live network 3G externals defined on Ericsson RNCs"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.gsm_external_cells
        (pk, name, cell_pk, node_pk, mcc, mnc, lac, bcch, ncc, bcc, ci, rac, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
        t1."userLabel" AS "name",
        t3.pk AS cell_pk,
        t2.pk AS node_pk,
        t1."mcc"::integer AS mcc,
        t1."mnc"::integer AS mnc,
        t1."lac"::integer AS lac,
        t1."bcchFrequency"::integer AS bcch,
        t1."ncc"::integer AS ncc,
        t1."bcc"::integer AS bcc,
        t1."cellIdentity"::integer AS ci,
        t1."rac"::integer AS rac,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        ericsson_cm_3g."ExternalGsmCell" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = 'SubNetwork' and t2.vendor_pk = 1 and t2.tech_pk = 3
        LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
        LEFT JOIN live_network.gsm_external_cells t4 on t4."name" = t1."userLabel" 
            AND t4.lac = t1."lac"::integer
            AND t4.ci = t1."cellIdentity"::integer
        WHERE 
        t4.pk IS NULL
        """
        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_3g_externals_on_4g(self):
        """Extract network externals defined on Ericsson 4G"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.umts_external_cells
        (pk, name, cell_pk,  mcc, mnc, lac, rac, rncid, ci, psc, uarfcn_dl, uarfcn_ul, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_umts_external_cells_pk') as pk,
        t1."userLabel" AS "name",
        t3.pk AS cell_pk,
        t1."mcc"::integer AS mcc,
        t1."mnc"::integer AS mnc,
        t1."lac"::integer AS lac,
        t1."rac"::integer AS rac,
        t1."rncId"::integer as rncid,
        t1."cId"::integer as ci,
        t1."primaryScramblingCode"::integer AS psc,
        t1."uarfcnDl"::integer AS uarfcn_dl,
        t1."uarfcnUl"::integer AS uarfcn_ul,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        ericsson_cm_3g."ExternalUtranCell" t1
        LEFT JOIN live_network.nodes t2 ON t2."name" = 'SubNetwork' and t2.vendor_pk = 1 AND tech_pk = 3
        LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
        LEFT JOIN live_network.umts_external_cells t4 on t4."name" = t1."userLabel" 
            AND t4.lac = t1."lac"::integer
        WHERE 
        t4.pk IS NULL
        """
        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_4g_externals_on_4g(self):
        """Extract network externals defined on Ericsson 4G"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
        INSERT INTO live_network.lte_external_cells
        (pk, name, node_pk, cell_pk, mcc, mnc, pci, dl_earfcn, ci, modified_by, added_by, date_added, date_modified)
        SELECT 
        NEXTVAL('live_network.seq_gsm_external_cells_pk') as pk,
        t1."userLabel" AS "name",
        t5.pk as node_pk,
        t3.pk AS cell_pk,
        null AS mcc,
        null AS mnc,
        (t1."localCellId"::integer + 3 * t1."physicalLayerCellIdGroup"::integer) AS pci,
        t1."earfcndl"::integer AS dl_earfcn_dl,
        t1."localCellId"::integer AS ci,
        0 as modified_by,
        0 as added_by,
        now()::timestamp as date_added,
        now()::timestamp as date_modified
        FROM
        ericsson_cm_3g."ExternalEUtranCellFDD" t1
        LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
        LEFT JOIN live_network.lte_external_cells t4 on t4."name" = t1."userLabel" 
            AND t4.ci = t1."localCellId"::integer
        LEFT JOIN live_network.nodes t5 ON t5.name = 'SubNetwork' AND t5.vendor_pk = 1 AND t5.tech_pk = 3
        WHERE 
        t4.pk IS NULL
        """
        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_live_network_2g_externals_on_4g(self):
        pass

    def extract_live_network_externals_on_2g(self):
        """Extract live network externals defined on Ericsson 2G"""
        self.extract_live_network_2g_externals_on_2g()
        self.extract_live_network_3g_externals_on_2g()
        self.extract_live_network_4g_externals_on_2g()

    def extract_live_network_externals_on_3g(self):
        """Extract live network externals defined on Ericsson 3G"""
        self.extract_live_network_2g_externals_on_3g()
        self.extract_live_network_3g_externals_on_3g()
        self.extract_live_network_4g_externals_on_3g()

    def extract_live_network_externals_on_4g(self):
        """Extract live network externals defined on Ericsson 4G"""
        self.extract_live_network_2g_externals_on_4g()
        self.extract_live_network_3g_externals_on_4g()
        self.extract_live_network_4g_externals_on_4g()