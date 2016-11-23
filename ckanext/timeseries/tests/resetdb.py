import json
import nose
import pprint
import time
import datetime
import pytz

import pylons
import sqlalchemy.orm as orm

import ckan.plugins as p
import ckan.lib.create_test_data as ctd
import ckan.model as model
import ckan.tests.legacy as tests

import ckanext.timeseries.db as db
from ckanext.timeseries.tests.helpers import extract, rebuild_all_dbs
from ckanext.timeseries.helpers import utcnow

import ckan.tests.helpers as helpers
import ckan.tests.factories as factories

assert_equals = nose.tools.assert_equals
assert_raises = nose.tools.assert_raises

class TestDatastore_TsSearch(tests.WsgiAppCase):
    sysadmin_user = None
    normal_user = None

    @classmethod
    def setup_class(cls):
        engine = db._get_engine(
                {'connection_url': pylons.config['ckan.datastore.write_url']}
            )
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

    @classmethod
    def teardown_class(cls):
        rebuild_all_dbs(cls.Session)
        # p.unload('timeseries')

    def test_search_timeseries(self):
        assert True