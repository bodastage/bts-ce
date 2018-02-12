from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import json


class Utils(object):
    """Utility class"""

    def __init__(self, dbname = None, dbuser = None, dbpass = None, dbhost = None):
        """ Constructor for this class. """

        self._dbhost=dbhost
        self._dbname=dbname
        self._dbuser=dbuser
        self._dbpass=dbpass

        if dbname is None: self._dbname="bts"
        if dbuser is None: self._dbuser="bodastage"
        if dbpass is None: self._dbpass="password"
        if dbhost is None: self._dbhost="locahost"
        self.db_engine = create_engine('postgresql://bodastage:password@database/bts')

    def truncate_schema_tables(self, schema = "public", tables = None):
        """Truncate cm tables"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        sql="""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '{0}'
        """.format(schema)

        result = self.db_engine.execute(sql)

        for row in result:
            table=row[0]
            qry="truncate {0}.{1}".format(schema,table)
            self.db_engine.execute(text(qry).execution_options(autocommit=True))

    def reset_database(self):
        """Reset the entire database"""
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        # Truncate relations table
        self.db_engine.execute(
            text("TRUNCATE TABLE live_network.relations;").execution_options(
                autocommit=True))
        self.db_engine.execute(
           text("ALTER SEQUENCE live_network.seq_relations_pk RESTART WITH 1;").execution_options(
               autocommit=True))

        # Truncate cell parameter values
        self.db_engine.execute(
            text("TRUNCATE TABLE live_network.umts_cells_data;").execution_options(
                autocommit=True))
        self.db_engine.execute(
            text("ALTER SEQUENCE live_network.seq_umts_cells_data_pk RESTART WITH 1;").execution_options(
                autocommit=True))

        # Truncate cells
        self.db_engine.execute(
            text("TRUNCATE TABLE live_network.cells;").execution_options(
                autocommit=True))
        self.db_engine.execute(
            text("ALTER SEQUENCE live_network.seq_cells_pk RESTART WITH 1;").execution_options(
                autocommit=True))

        # Truncate sites
        self.db_engine.execute(
            text("TRUNCATE TABLE live_network.sites;").execution_options(
                autocommit=True))
        self.db_engine.execute(
            text("ALTER SEQUENCE live_network.seq_sites_pk RESTART WITH 1;").execution_options(
                autocommit=True))

        # Truncate nodes
        self.db_engine.execute(
            text("TRUNCATE TABLE live_network.nodes;").execution_options(
                autocommit=True))
        self.db_engine.execute(
            text("ALTER SEQUENCE live_network.seq_nodes_pk RESTART WITH 1;").execution_options(
                autocommit=True))

    def build_mo_aci_tree(self):
        """Build and cache mo tree for managed objects"""

        # Get all mos: name, pk, and parent_pk

        engine = create_engine('postgresql://{0}:{1}@{2}/{3}'.format(self._dbuser, self._dbpass, self._dbhost, self._dbname))

        Session = sessionmaker(bind=engine)
        session = Session()

        # This is for Ericsson 3G4G only
        mos_dict = {}
        result = session.execute("""SELECT pk, name, parent_pk FROM managedobjects WHERE vendor_pk = 1 AND tech_pk = 2""")

        for row in result:
            mos_dict[row['name']] = {"pk":row['pk'],"parent_pk":row['parent_pk']}

        # Recursively assemble tree
        def assemble_tree(mosdict, root=0):
            nodes = []
            for k, v in mosdict.items():

                if v['parent_pk'] == root:
                    branch = assemble_tree(mos_dict, v['pk'])
                    nodes.append({
                        "id": v['pk'],
                        "label": k,
                        "inode": len(branch) > 0 ,
                        "open": False,
                        "branch": branch
                    })
            return nodes

        aci_tree = json.dumps(assemble_tree(mos_dict,0))

        stmt = text("""UPDATE cache 
                       SET data = :tree, date_modified = now()::timestamp 
                       WHERE 
                       name = 'mo_aci_tree'
                       """)
        stmt = stmt.execution_options(autocommit=True).bindparams(tree=aci_tree)
        engine.execute(stmt)

    def build_live_network_aci_tree(self):
        """Build network tree"""

        engine = create_engine('postgresql://{0}:{1}@{2}/{3}'.format(self._dbuser, self._dbpass, self._dbhost, self._dbname))

        Session = sessionmaker(bind=engine)
        session = Session()

        metadata = MetaData()
        nodes_table = Table('nodes', metadata, autoload=True, autoload_with=engine, schema='live_network')
        cells_table = Table('cells', metadata, autoload=True, autoload_with=engine, schema='live_network')
        sites_table = Table('sites', metadata, autoload=True, autoload_with=engine, schema='live_network')


        aci_tree = [
            {"id": "msc_root", "name": "MSCs", "inode": True, "open": False, "branch":[] },
            {"id": "bsc_root", "name": "BSCs", "inode": True, "open": False, "branch":[]},
            {"id": "rnc_root", "name": "RNCs", "inode": True, "open": False, "branch":[]},
            {"id": "enodeb_root", "name": "ENodeBs", "inode": True, "open": False, "branch":[]}
        ]

        def assemble_network_tree(parent='root',type=None):
            nodes = []

            if parent == 'root':
                bsc_count = session.query(nodes_table).filter_by(type="BSC",tech_pk=1).count()
                msc_count = session.query(nodes_table).filter_by(type="MSC", tech_pk=1).count()
                rnc_count = session.query(nodes_table).filter_by(type="RNC", tech_pk=2).count()
                enodeb_count = session.query(sites_table).filter_by(tech_pk=3).count()

                nodes = [
                    {"id": "msc_root", "label": "MSCs({})".format(msc_count), "inode": True, "open": False,
                     "branch": assemble_network_tree("msc_root")},
                    {"id": "bsc_root", "label": "BSCs({})".format(bsc_count), "inode": True, "open": False,
                     "branch": assemble_network_tree("bsc_root")},
                    {"id": "rnc_root", "label": "RNCs({})".format(rnc_count), "inode": True, "open": False,
                     "branch": assemble_network_tree("rnc_root")},
                    {"id": "enodeb_root", "label": "ENodeBs({})".format(enodeb_count), "inode": True, "open": False,
                     "branch": assemble_network_tree("enodeb_root")}
                ]

            if parent == 'msc_root':
                for row in session.query(nodes_table).filter_by(type="MSC",tech_pk=1).all():
                    nodes.append({
                        "id": row.pk,
                        "label": row.name,
                        "inode": False,
                        "open": False
                    })

            if parent == 'bsc_root':
                for row in session.query(nodes_table).filter_by(type="BSC",tech_pk=1).all():
                    site_count = session.query(sites_table).filter_by(tech_pk=1, node_pk=row.pk).count()
                    nodes.append({
                        "id": row.pk,
                        "label": "{}({})".format(row.name,site_count),
                        "inode": True,
                        "open": False,
                        "branch": assemble_network_tree(row.pk, 'bsc')
                    })

            if parent == 'rnc_root':
                for row in session.query(nodes_table).filter_by(type="RNC",tech_pk=2).all():
                    site_count = session.query(sites_table).filter_by(tech_pk=2, node_pk=row.pk).count()
                    nodes.append({
                        "id": row.pk,
                        "label": "{}({})".format(row.name,site_count),
                        "inode": True,
                        "open": False,
                        "branch": assemble_network_tree(row.pk, 'rnc')
                    })

            if parent == 'enodeb_root':
                for row in session.query(sites_table).filter_by(tech_pk=3).all():
                    cell_count = session.query(cells_table).filter_by(tech_pk=3, site_pk=row.pk).count()
                    nodes.append({
                        "id": row.pk,
                        "label": "{}({})".format(row.name,cell_count),
                        "inode": True,
                        "open": False,
                        "branch": assemble_network_tree(row.pk, 'enodeb')
                    })

            # Parent node is an RNC
            if type == 'rnc':
                for row in session.query(sites_table).filter_by(tech_pk=2,node_pk=parent).all():
                    cell_count = session.query(cells_table).filter_by(tech_pk=2, site_pk=row.pk).count()
                    nodes.append({
                        "id": row.pk,
                        "label": "{}({})".format(row.name,cell_count),
                        "inode": True,
                        "open": False,
                        "branch": assemble_network_tree(row.pk, 'nodeb')
                    })

            # Parent node is an NodeB
            if type == 'nodeb':
                for row in session.query(cells_table).filter_by(tech_pk=2,site_pk=parent).all():
                    nodes.append({
                        "id": row.pk,
                        "label": row.name,
                        "inode": False,
                        "open": False,
                        # "branch": assemble_network_tree(row.pk, 'nodeb')
                    })

            # Parent node is an NodeB
            if type == 'enodeb':
                for row in session.query(cells_table).filter_by(tech_pk=3,site_pk=parent).all():
                    nodes.append({
                        "id": row.pk,
                        "label": row.name,
                        "inode": False,
                        "open": False,
                        # "branch": assemble_network_tree(row.pk, 'eucell')
                    })

            # Parent node is a BSC
            if type == 'bsc':
                for row in session.query(sites_table).filter_by(tech_pk=1,node_pk=parent).all():
                    cell_count = session.query(cells_table).filter_by(tech_pk=1, site_pk=row.pk).count()
                    nodes.append({
                        "id": row.pk,
                        "label": "{}({})".format(row.name,cell_count),
                        "inode": True,
                        "open": False,
                        "branch": assemble_network_tree(row.pk, 'site')
                    })

            # Parent node is a site
            if type == 'site':
                for row in session.query(cells_table).filter_by(tech_pk=1,site_pk=parent).all():
                    nodes.append({
                        "id": row.pk,
                        "label": row.name,
                        "inode": False,
                        "open": False
                    })

            return nodes

        aci_tree = json.dumps(assemble_network_tree())
        print(aci_tree)

        stmt = text("""UPDATE cache 
                       SET data = :tree, date_modified = now()::timestamp 
                       WHERE 
                       name = 'live_network_aci_tree'
                       """)
        stmt = stmt.execution_options(autocommit=True).bindparams(tree=aci_tree)
        engine.execute(stmt)

    def get_setting(self, name, default=None):
        """Get the value of a setting """
        Session = sessionmaker(bind=self.db_engine)
        session = Session()

        metadata = MetaData()
        settings = Table('settings', metadata, autoload=True, autoload_with=self.db_engine, schema='public')
        setting = session.query(settings).filter_by(name=name).first()

        data_type = setting.data_type
        value = None

        if data_type == 'string': value = setting.string_value
        if data_type == 'integer': value = setting.integer_value
        if data_type == 'float': value  = setting.float_value
        if data_type == 'timestamp': value  = setting.timestamp_value

        if value is not None:
            return value
        else:
            return default
