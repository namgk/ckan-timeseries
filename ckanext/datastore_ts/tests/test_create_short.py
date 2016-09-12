import json
import nose
import sys
from nose.tools import assert_equal, raises

import pylons
from pylons import config
import sqlalchemy.orm as orm
import paste.fixture

import ckan.plugins as p
import ckan.lib.create_test_data as ctd
import ckan.model as model
import ckan.tests.legacy as tests
import ckan.config.middleware as middleware
import ckan.tests.helpers as helpers
import ckan.tests.factories as factories

import ckanext.datastore_ts.db as db
from ckanext.datastore_ts.tests.helpers import rebuild_all_dbs, set_url_type

# avoid hanging tests https://github.com/gabrielfalcao/HTTPretty/issues/34
if sys.version_info < (2, 7, 0):
    import socket
    socket.setdefaulttimeout(1)

class TestDatastoreCreateNewTests(object):
    @classmethod
    def setup_class(cls):
        p.load('datastore_ts')

    @classmethod
    def teardown_class(cls):
        p.unload('datastore_ts')
        helpers.reset_db()

    def _get_index_names(self, resource_id):
        sql = u"""
            SELECT
                i.relname AS index_name
            FROM
                pg_class t,
                pg_class i,
                pg_index idx
            WHERE
                t.oid = idx.indrelid
                AND i.oid = idx.indexrelid
                AND t.relkind = 'r'
                AND t.relname = %s
            """
        results = self._execute_sql(sql, resource_id).fetchall()
        return [result[0] for result in results]


class TestDatastoreCreate(tests.WsgiAppCase):
    sysadmin_user = None
    normal_user = None

    @classmethod
    def setup_class(cls):

        wsgiapp = middleware.make_app(config['global_conf'], **config)
        cls.app = paste.fixture.TestApp(wsgiapp)
        if not tests.is_datastore_supported():
            raise nose.SkipTest("Datastore not supported")
        p.load('datastore_ts')
        ctd.CreateTestData.create()
        cls.sysadmin_user = model.User.get('testsysadmin')
        cls.normal_user = model.User.get('annafan')
        engine = db._get_engine(
            {'connection_url': pylons.config['ckan.datastore.write_url']})
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))
        set_url_type(
            model.Package.get('annakarenina').resources, cls.sysadmin_user)

    @classmethod
    def teardown_class(cls):
        rebuild_all_dbs(cls.Session)
        p.unload('datastore_ts')

    def test_empty(self):
        pass
