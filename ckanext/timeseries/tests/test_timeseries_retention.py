# encoding: utf-8

import json
import nose
import urllib
import pprint
import time

import sqlalchemy.orm as orm

import ckan.plugins as p
import ckan.lib.create_test_data as ctd
import ckan.model as model
import ckan.tests.legacy as tests

from ckanext.timeseries.helpers import utcnow

from ckan.common import config
import ckanext.timeseries.backend.postgres as db
from ckanext.timeseries.tests.helpers import (
    extract, rebuild_all_dbs, set_url_type,
    DatastoreFunctionalTestBase, DatastoreLegacyTestBase)

import ckan.tests.helpers as helpers
import ckan.tests.factories as factories

assert_equals = nose.tools.assert_equals
assert_raises = nose.tools.assert_raises
assert_in = nose.tools.assert_in


class TestTimeseriesRetention(DatastoreLegacyTestBase):
    sysadmin_user = None
    normal_user = None

    @classmethod
    def setup_class(cls):
        cls.app = helpers._get_test_app()
        super(TestTimeseriesRetention, cls).setup_class()
        # Creating 3 resources with 3 retention policies: 
        # to remove 10, 20 and 90% of data when the 
        # resource gets to its size limit
        cls.retention = [10, 90, 20, 50]
        cls.resource_ids = []

        package = factories.Dataset()
        for i, ret in enumerate(cls.retention):
            data = {
                'resource': {
                    'retention': cls.retention[i],
                    'package_id': package['id']
                },
            }
            result = helpers.call_action('timeseries_create', **data)
            cls.resource_ids.append(result['resource_id'])

        engine = db.get_write_engine()
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

    def test_retention_in_action(self):
        sql_resource_count = 'select min("_id"), count("_id") \
            from "{}" '
        ret_idx = 2 # min: 0, max: 3
    
        # records = []
        # for i in range(0, 500):
        #     records.append({'author': 'tolstoy7tolstoy7',
        #                 'published': '2005-03-01'})
        data = {
            'resource_id': self.resource_ids[ret_idx],
            'force': True,
            'fields': [{'id': 'author', 'type': 'text'},
                       {'id': 'published'}],
            'records': [
                {'author': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
                aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
                aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
                aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                'published': '2005-03-01'}
                for i in range(0,2500)
             ]
        }

        result = helpers.call_action('timeseries_create', **data)
        del data['fields']
        data['method'] = 'insert'
        result = helpers.call_action('timeseries_upsert', **data)
        size = db._get_resource_size(self.resource_ids[ret_idx], self.Session.connection())
        
        min_count = self.Session.connection().execute(sql_resource_count.format(self.resource_ids[ret_idx])).fetchone()
        min_id = int(min_count[0])
        count = int(min_count[1])

        print(count, min_id)

        assert min_id != 1
        assert count < 5000

        # TODO: create another test for is_timeseries
        assert db._is_timeseries({"connection": self.Session.connection()}, data['resource_id'])
