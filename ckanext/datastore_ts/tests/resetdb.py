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

import ckanext.datastore_ts.db as db
from ckanext.datastore_ts.tests.helpers import extract, rebuild_all_dbs
from ckanext.datastore_ts.helpers import utcnow

import ckan.tests.helpers as helpers
import ckan.tests.factories as factories

assert_equals = nose.tools.assert_equals
assert_raises = nose.tools.assert_raises

class TestDatastore_TsSearch(tests.WsgiAppCase):
    sysadmin_user = None
    normal_user = None

    @classmethod
    def setup_class(cls):
        if not tests.is_datastore_supported():
            raise nose.SkipTest("Datastore not supported")
        p.load('datastore_ts')
        cls.sysadmin_user = model.User.get('testsysadmin')
        cls.normal_user = model.User.get('annafan')
        cls.dataset = model.Package.get('annakarenina')
        cls.resource = cls.dataset.resources[0]

        # Make an organization, because private datasets must belong to one.
        cls.organization = tests.call_action_api(
            cls.app, 'organization_create',
            name='test_org',
            apikey=cls.sysadmin_user.apikey)

        engine = db._get_engine(
                {'connection_url': pylons.config['ckan.datastore.write_url']}
            )
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

        rebuild_all_dbs(cls.Session)
        time.sleep(3333)


    @classmethod
    def teardown_class(cls):
        rebuild_all_dbs(cls.Session)
        p.unload('datastore_ts')

    def test_search_timeseries(self):
        pass