import json
import nose
import pprint
import time
import pylons
import sqlalchemy.orm as orm

import ckan.plugins as p
import ckan.lib.create_test_data as ctd
import ckan.model as model
import ckan.tests.legacy as tests

import ckanext.datastore.db as db
from ckanext.datastore.tests.helpers import extract, rebuild_all_dbs

import ckan.tests.helpers as helpers
import ckan.tests.factories as factories

assert_equals = nose.tools.assert_equals
assert_raises = nose.tools.assert_raises

class ResetTestDb(tests.WsgiAppCase):

    @classmethod
    def setup_class(cls):
        if not tests.is_datastore_supported():
            raise nose.SkipTest("Datastore not supported")
        p.load('datastore_ts')

        engine = db._get_engine(
            {'connection_url': pylons.config['ckan.datastore.write_url']}
        )
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

        rebuild_all_dbs(cls.Session)

    @classmethod
    def teardown_class(cls):
        p.unload('datastore_ts')

    def test_search_timeseries(self):
        pass