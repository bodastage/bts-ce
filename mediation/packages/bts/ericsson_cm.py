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
