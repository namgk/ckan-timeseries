import mock
import json
import nose
import time
import sqlalchemy.exc

import ckan.plugins as p
import ckan.tests.helpers as helpers
import ckan.tests.factories as factories
import ckan.model as model
import ckan.lib.create_test_data as ctd

assert_equal = nose.tools.assert_equal

import pylons
import sqlalchemy.orm as orm

import ckanext.timeseries.helpers as datastore_helpers
import ckanext.timeseries.tests.helpers as datastore_test_helpers
import ckanext.timeseries.db as db
import ckanext.timeseries.commands as cmd

class TestCommands(object):

    @classmethod
    def setup_class(cls):
        p.load('timeseries')
        helpers.reset_db()
        ctd.CreateTestData.create()
        cls.sysadmin_user = model.User.get('testsysadmin')
        cls.normal_user = model.User.get('annafan')
        engine = db._get_engine(
            {'connection_url': pylons.config['ckan.datastore.write_url']})
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))
        cls.dataset = model.Package.get('annakarenina')

        cls.data = {
            'resource_id': '',
            'force': True,
            'method': 'insert',
            'records': [{'author': 'tolstoy5',
                        'published': '2005-03-05'},
                        {'author': 'tolstoy6'},
                        {'author': 'tolstoy7',
                        'published': '2005-03-05'}
                       ]
        }

        cls.data['resource_id'] = cls.dataset.resources[0].id
        result = helpers.call_action('datastore_ts_create', **cls.data)

        cls.data['resource_id'] = cls.dataset.resources[1].id
        result = helpers.call_action('datastore_ts_create', **cls.data)

        datastore_test_helpers.set_url_type(
            model.Package.get('annakarenina').resources, cls.sysadmin_user)

    @classmethod
    def teardown_class(cls):
        datastore_test_helpers.clear_db(cls.Session)
        p.unload('timeseries')
        helpers.reset_db()

    def test_migrate_autogen(self):
        conn = self.Session.connection()
        old_name = '_autogen_timestamp'
        new_name = 'autogen_timestamp'

        sql_describe = 'select column_name \
            from INFORMATION_SCHEMA.COLUMNS where table_name = %s'
        sql_autogen_res = 'select table_name \
            from INFORMATION_SCHEMA.COLUMNS where column_name = %s'
        sql_rename_column = 'ALTER TABLE "{table_name}" RENAME {old_name} TO {new_name}'
        
        autogen_res = conn.execute(sql_autogen_res, old_name).fetchall()
        for ar in autogen_res:
            columns = conn.execute(sql_describe, ar[0]).fetchall()
            columns = map(lambda x: x[0],columns)
            assert old_name in columns

        cmd._migrate_autogen_timestamp(old_name, new_name)

        sql_resource_names = "select name from _table_metadata"
        res_names = conn.execute(sql_resource_names).fetchall()
        for n in res_names:
            columns = conn.execute(sql_describe, n[0]).fetchall()
            columns =  map(lambda x: x[0],columns)
            if 'oid' not in columns:
                assert new_name in columns

        self.Session.remove()
