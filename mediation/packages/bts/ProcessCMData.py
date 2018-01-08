from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

class ProcessCMData(object):
    """ Process network configuration data"""

    def __init__(self, dbname = None, dbuser = None, dbpass = None, dbhost = None):
        ''' Constructor for this class. '''

        self._dbhost=dbhost
        self._dbname=dbname
        self._dbuser=dbuser
        self._dbpass=dbpass

        if dbname is None: self._dbname="bts"
        if dbuser is None: self._dbuser="bodastage"
        if dbpass is None: self._dbpass="password"
        if dbhost is None: self._dbhost="locahost"

        self.db_engine = create_engine('postgresql://{0}:{1}@{2}/{3}'.format(self._dbuser, self._dbpass, self._dbhost, self._dbname))

    def extract_rncs(self, vendor= None):
        """Extract RNCs from provided vendors or else get from all vendors"""

        if vendor == None or vendor == 'ericsson':
            self.extract_ericsson_rncs()

    def extract_ericsson_rncs(self):
        """Extract RNCs from ericsson CM data"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.nodes
            (pk,date_added, date_modified, type,"name", vendor_pk, tech_pk, added_by, modified_by)
            SELECT 
            NEXTVAL('live_network.seq_nodes_pk'),
            "varDateTime" as date_added, 
            "varDateTime" as date_modified, 
            'RNC' as node_type,
            "MeContext_id" as "name" , 
            1 as vendor_pk, -- 1=Ericsson, 2=Huawei
            2 as tech_pk , -- 1=gsm, 2-umts,3=lte
            0 as added_by,
            0 as modified_by
            FROM eri_cm_3g4g.rncfunction t1
            LEFT OUTER  JOIN live_network.nodes t2 ON t1."MeContext_id" = t2."name"
            WHERE 
            t2."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_enodebs(self):
        """Extract Ericsson ENodebs"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.sites
            (pk, date_added,date_modified, tech_pk, vendor_pk, "name", added_by, modified_by)
            SELECT 
            NEXTVAL('live_network.seq_sites_pk'),
            "varDateTime" as date_added, 
            "varDateTime" as date_modified, 
            3 as tech_pk , -- 1=gsm, 2-umts,3=lte,
            1 as vendor_pk, -- 1=Ericsson, 2=Huawei
            "MeContext_id",
            0 as added_by,
            0 as modified_by
            FROM eri_cm_3g4g.vsDataENodeBFunction t1
            LEFT OUTER  JOIN live_network.sites t2 ON t1."MeContext_id" = t2."name"
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
            from eri_cm_3g4g.nodebfunction t1
            INNER join live_network.nodes t2 on t2."name" = t1."SubNetwork_2_id" 
                AND t2.vendor_pk = 1 and t2.tech_pk = 2
            LEFT JOIN live_network.sites t3 on t3."name" = t1."MeContext_id" 
               AND t2.vendor_pk = 1 and t2.tech_pk = 2
            WHERE 
            t3."name" IS NULL
            
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g_cells(self):
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
            FROM eri_cm_3g4g.utranCell t1
            INNER JOIN eri_cm_3g4g.nodebfunction t2 on t2."nodeBFunctionIubLink" = t1."utranCellIubLink"
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

    def extract_ericsson_4g_cells(self):
        """Extract Ericsson LTE Cells"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.cells
            (pk, date_added,date_modified,added_by, modified_by, tech_pk, vendor_pk, name, site_pk)
            SELECT 
            NEXTVAL('live_network.seq_cells_pk'),
            t1."varDateTime" as date_added, 
            t1."varDateTime" as date_modified, 
            0 as added_by,
            0 as modified_by,
            3, -- tech 3 -lte, 2 -umts, 1-gms
            1, -- 1- Ericsson, 2 - Huawei, 3 - ZTE, 4-Nokia
            t1."userLabel",
            t2.pk -- site primary key
            FROM eri_cm_3g4g.vsdataeutrancellfdd t1
            INNER JOIN live_network.sites t2 on t2."name" = t1."MeContext_id" 
                AND t2.vendor_pk = 1 and t2.tech_pk = 3 
            LEFT JOIN live_network.cells t3 on t3."name" = t1."userLabel"
                AND t2.vendor_pk = 1 and t2.tech_pk = 3 
            WHERE
                t3."name" IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_4g_cell_params(self):
        """Extract Ericsson LTE cell parameters"""
        pass

    def extract_ericsson_3g_cell_params(self):
        """Extract Ericsson UMTS cell parameters"""

        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # Truncate paramete table
        self.db_engine.execute(text("TRUNCATE TABLE live_network.umts_cells_data").execution_options(autocommit=True))
        self.db_engine.execute(text("ALTER SEQUENCE live_network.seq_umts_cells_data_pk RESTART WITH 1;").execution_options(autocommit=True))

        # The data is alot. Let's handle per site
        site_sql = """SELECT pk, "name" from live_network.sites where vendor_pk  = 1 and tech_pk = 2"""

        result = self.db_engine.execute(site_sql)

        for row in result:
            (site_pk,site_name)=row

            print("Extracting cells parameters for site_pk: {0}, site_name: {1}".format(site_pk,site_name))

            sql = """
                INSERT INTO live_network.umts_cells_data
                (pk, date_added, date_modified, added_by, modified_by,bch_power,cell_id,cell_pk,lac,latitude, longitude, maximum_transmission_power, "name", notes, cpich_power, primary_sch_power, scrambling_code, rac, sac, secondary_sch_power, site_pk, tech_pk, vendor_pk, uarfcn_dl,uarfcn_ul, ura_list, azimuth, cell_range, height, site_sector_carrier)
                SELECT 
                NEXTVAL('live_network.seq_umts_cells_data_pk'),
                t1."varDateTime" as date_added, 
                t1."varDateTime" as date_modified, 
                0 as added_by,
                0 as modified_by,
                t1."bchPower"::integer,
                t1."cId"::integer,
                t3.pk as cell_pk, -- cellid
                t1."lac"::integer,
                (t4."antennaPosition_latitude"::float/93206.76)*(-1::float*t4."antennaPosition_latitudeSign"::float) as latitude,
                t4."antennaPosition_longitude"::float/46603.38 as longitude,
                t1."maximumTransmissionPower"::integer as maximum_transmission_power,
                t1."UtranCell_id",
                '',
                t1."primaryCpichPower"::integer as cpich_power,
                t1."primarySchPower"::integer as primary_sch_power,
                t1."primaryScramblingCode"::integer as scrambling_code,
                t1."rac"::integer,
                t1."sac"::integer,
                t1."secondarySchPower"::integer as secondary_sch_power,
                t3.site_pk, -- site pk
                2, -- umts
                1, -- Ericsson
                t1."uarfcnDl"::integer,
                t1."uarfcnUl"::integer,
                t1."uraList",
                t6."beamDirection"::integer, -- azimuth,
                t5."cellRange"::integer, -- cellrange,
                t6."height"::integer, -- height
                concat(t2."MeContext_id", '_', t2."vsDataRbsLocalCell_id") as site_sector_carrier
                FROM 
                eri_cm_3g4g.utrancell t1
                INNER JOIN eri_cm_3g4g.vsdatarbslocalcell t2 on t2."localCellId" = t1."cId" and t2."SubNetwork_2_id" = t1."SubNetwork_2_id" 
                    -- and t2."MeContext_id" = t1."MeContext_id"
                INNER JOIN live_network.cells t3 on t3."name" = t1."UtranCell_id"
                INNER JOIN eri_cm_3g4g.vsDataUtranCell t4 on t4."UtranCell_id" = t1."UtranCell_id" and t4."SubNetwork_2_id" = t1."SubNetwork_2_id" 
                INNER JOIN eri_cm_3g4g.vsdatacarrier t5 on  t5."SubNetwork_2_id" = t1."SubNetwork_2_id" 
                    and t5."MeContext_id" = t2."MeContext_id"
                    and concat('S',TRIM(t5."vsDataSector_id"),'C', TRIM(t5."vsDataCarrier_id")) = t2."vsDataRbsLocalCell_id" 
                INNER JOIN eri_cm_3g4g.vsdatasector t6 on t6."SubNetwork_2_id" = t1."SubNetwork_2_id"
                    and t6."MeContext_id" = t5."MeContext_id"
                    and t6."vsDataSector_id" = t5."vsDataSector_id"
                WHERE t6."MeContext_id" = '{0}';
            """.format(site_name)

            self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g2g_nbrs(self):
        """Extract Ericsson UMTS-GSM neighbour relations"""
        pass

    def extract_ericsson_3g3g_nbrs(self):
        """Extract Ericsson UMTS-UMTS neighbour relations"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql = """
            INSERT INTO live_network.relations 
            (pk, svrnode_pk,svrsite_pk,svrtech_pk,svrvendor_pk,svrcell_pk,nbrnode_pk,nbrsite_pk,nbrtech_pk, nbrvendor_pk,nbrcell_pk,date_added,date_modified, added_by, modified_by)
            SELECT 
            NEXTVAL('live_network.seq_relations_pk'),
            -- serving side
            t4.node_pk as svrnode_pk, 
            t3.site_pk as svrsite_pk, 
            t3.tech_pk as svrtech_pk,
            t3.vendor_pk as svrvendor_pk ,
            t3.pk as svrcell_pk,
            -- nbr side 
            t7.node_pk as nbrnode_pk, 
            t6.site_pk as nbrsite_pk, 
            t6.tech_pk as nbrtech_pk,
            t6.vendor_pk as nbrvendor_pk ,
            t6.pk as nbrcell_pk,
            -- meta fields 
            t1."varDateTime" ,
            t1."varDateTime" ,
            0, -- system
            0
            FROM eri_cm_3g4g.utranrelation t1 
            INNER JOIN eri_cm_3g4g.utrancell t2 ON t1."adjacentCell" = concat('SubNetwork=ONRM_ROOT_MO_R,SubNetwork=',trim(t2."SubNetwork_2_id"),',MeContext=',trim(t2."MeContext_id"),',ManagedElement=',trim(t2."ManagedElement_id"),',RncFunction=',trim(t2."RncFunction_id"),',UtranCell=',trim(t2."UtranCell_id"))
            -- serving side
            INNER JOIN live_network.cells t3 ON t3."name" = t1."UtranCell_id" AND t3.vendor_pk = 1 AND t3.tech_pk = 2
            INNER JOIN live_network.sites t4 ON t4.pk = t3.site_pk
            INNER JOIN live_network.nodes t5 ON t5.pk = t4.node_pk 
            -- nbr side 
            INNER JOIN live_network.cells t6 ON t6."name" = t1."UtranRelation_id" AND t3.vendor_pk = 1 AND t3.tech_pk = 2
            INNER JOIN live_network.sites t7 ON t7.pk = t6.site_pk
            -- This part is to extract only new relations
            LEFT JOIN live_network.relations t9 ON t9.svrcell_pk = t3.pk 
                AND t9.nbrcell_pk = t6.pk
            WHERE 
                t9.pk IS NULL
        """

        self.db_engine.execute(text(sql).execution_options(autocommit=True))

        session.close()

    def extract_ericsson_3g4g_nbrs(self):
        """Extract Ericsson UMTS-LTE neighbour relations"""
        pass

    def extract_ericsson_4g4g_nbrs(self):
        """Extract Ericsson LTE-LTE neighbour relations"""
        pass