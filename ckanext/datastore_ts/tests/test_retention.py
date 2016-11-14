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

import ckanext.datastore_ts.db as db
from ckanext.datastore_ts.tests.helpers import extract, rebuild_all_dbs
from ckanext.datastore_ts.helpers import utcnow

import ckan.tests.helpers as helpers
import ckan.tests.factories as factories

assert_equals = nose.tools.assert_equals
assert_raises = nose.tools.assert_raises

class TestRetentionPolicy(tests.WsgiAppCase):
    sysadmin_user = None
    normal_user = None

    @classmethod
    def setup_class(cls):
        if not tests.is_datastore_supported():
            raise nose.SkipTest("Datastore not supported")
        p.load('datastore_ts')
        helpers.reset_db()

        cls.retention = range(10,90,20)
        cls.resource_ids = []

        package = factories.Dataset()
        for i, ret in enumerate(cls.retention):
            data = {
                'resource': {
                    'retention': cls.retention[i],
                    'package_id': package['id']
                },
            }
            result = helpers.call_action('datastore_ts_create', **data)
            cls.resource_ids.append(result['resource_id'])

            data1 = {
                'resource_id': cls.resource_ids[i],
                'force': True,
                'fields': [{'id': 'author', 'type': 'text'},
                           {'id': 'published'}],
                'records': [{'author': 'tolstoy1',
                            'published': '2005-03-01'},
                            {'author': 'tolstoy2'},
                            {'author': 'tolstoy3',
                            'published': '2005-03-03'},
                            {'author': 'tolstoy4'},
                            {'author': 'tolstoy5',
                            'published': '2005-03-05'},
                            {'author': 'tolstoy6'},
                            {'author': 'tolstoy7',
                            'published': '2005-03-05'}
                           ]
            }

            result = helpers.call_action('datastore_ts_create', **data1)

        engine = db._get_engine(
                {'connection_url': pylons.config['ckan.datastore.write_url']}
            )
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

    @classmethod
    def teardown_class(cls):
        rebuild_all_dbs(cls.Session)
        p.unload('datastore_ts')
        cls.Session.remove()

    def test_retention_in_action(self):
        sql_resource_count = 'select min("_id"), count("_id") \
        from "{}" '
        ret_idx = 3 # min: 0, max: 3
    
        records = []
        for i in range(0, 500):
            records.append({'author': 'tolstoy7tolstoy7',
                        'published': '2005-03-01'})
        data = {
            'resource_id': self.resource_ids[ret_idx],
            'force': True,
            'fields': [{'id': 'author', 'type': 'text'},
                       {'id': 'published'}],
            'records': records
        }

        result = helpers.call_action('datastore_ts_create', **data)
        
        sql_resource_count = 'select min("_id"), count("_id") \
            from "{}" '
        min_count = self.Session.connection().execute(sql_resource_count.format(self.resource_ids[ret_idx])).fetchone()
        min_id = int(min_count[0])
        count = int(min_count[1])

        assert min_id != 1
        assert count < 500

    def test_get_resource_size(self):
        size = db._get_resource_size(self.resource_ids[0], self.Session.connection())
        assert size == 8192

    def test_cleanup_resource(self):
        sql_resource_count = 'select min("_id"), count("_id") \
        from "{}" '

        for i, ret in enumerate(self.retention):
            min_count = self.Session.connection().execute(sql_resource_count.format(self.resource_ids[i])).fetchone()
            print(min_count)

            min_id = int(min_count[0])
            count = int(min_count[1])

            retention_amount = int(self.retention[i] * count / 100)

            db._cleanup_resource(self.resource_ids[i], self.Session.connection())

            min_count = self.Session.connection().execute(sql_resource_count.format(self.resource_ids[i])).fetchone()
            min_id2 = int(min_count[0])
            count2 = int(min_count[1])

            print(min_id, min_id2, count, count2, retention_amount)

            assert (min_id2 - min_id) == retention_amount
            assert (count - count2) == retention_amount


