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
        if not tests.is_datastore_supported():
            raise nose.SkipTest("Datastore not supported")
        p.load('timeseries')
        helpers.reset_db()

        ctd.CreateTestData.create()
        cls.sysadmin_user = model.User.get('testsysadmin')
        cls.normal_user = model.User.get('annafan')
        cls.dataset = model.Package.get('annakarenina')
        cls.resource = cls.dataset.resources[0]
        cls.data = {
            'resource_id': cls.resource.id,
            'force': True,
            'aliases': 'books3',
            'fields': [{'id': 'author', 'type': 'text'},
                       {'id': 'published'}],
            'records': [{'author': 'tolstoy1',
                        'published': '2005-03-01'},
                        {'author': 'tolstoy2'}
                       ]
        }
        cls.data2 = {
            'resource_id': cls.resource.id,
            'force': True,
            'method': 'insert',
            'records': [{'author': 'tolstoy3',
                        'published': '2005-03-03'},
                        {'author': 'tolstoy4'}
                       ]
        }
        cls.data3 = {
            'resource_id': cls.resource.id,
            'force': True,
            'method': 'insert',
            'records': [{'author': 'tolstoy5',
                        'published': '2005-03-05'},
                        {'author': 'tolstoy6'},
                        {'author': 'tolstoy7',
                        'published': '2005-03-05'}
                       ]
        }
        cls.startdata = utcnow()
        postparams = '%s=1' % json.dumps(cls.data)
        auth = {'Authorization': str(cls.sysadmin_user.apikey)}
        res = cls.app.post('/api/action/datastore_ts_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True

        cls.enddata = utcnow()
        cls.startdata2 = utcnow()
        time.sleep(2)

        postparams = '%s=1' % json.dumps(cls.data2)
        res = cls.app.post('/api/action/datastore_ts_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True

        cls.enddata2 = utcnow()
        cls.startdata3 = utcnow()
        time.sleep(2)

        postparams = '%s=1' % json.dumps(cls.data3)
        res = cls.app.post('/api/action/datastore_ts_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True
        
        cls.enddata3 = utcnow()

        # Make an organization, because private datasets must belong to one.
        cls.organization = tests.call_action_api(
            cls.app, 'organization_create',
            name='test_org',
            apikey=cls.sysadmin_user.apikey)

        cls.expected_records = [{u'published': u'2005-03-01T00:00:00',
                                 u'_id': 1,
                                 u'author': u'tolstoy1'},
                                {u'published': None,
                                 u'_id': 2,
                                 u'author': u'tolstoy2'},
                                 {u'published': u'2005-03-03T00:00:00',
                                 u'_id': 3,
                                 u'author': u'tolstoy3'},
                                {u'published': None,
                                 u'_id': 4,
                                 u'author': u'tolstoy4'},
                                 {u'published': u'2005-03-05T00:00:00',
                                 u'_id': 5,
                                 u'author': u'tolstoy5'},
                                {u'published': None,
                                 u'_id': 6,
                                 u'author': u'tolstoy6'},
                                 {u'published': u'2005-03-05T00:00:00',
                                 u'_id': 7,
                                 u'author': u'tolstoy7'}]
        cls.expected_records1 = [{u'published': u'2005-03-01T00:00:00',
                                 u'_id': 1,
                                 u'author': u'tolstoy1'},
                                {u'published': None,
                                 u'_id': 2,
                                 u'author': u'tolstoy2'}]
        cls.expected_records12 = [{u'published': u'2005-03-01T00:00:00',
                                 u'_id': 1,
                                 u'author': u'tolstoy1'},
                                {u'published': None,
                                 u'_id': 2,
                                 u'author': u'tolstoy2'},
                                 {u'published': u'2005-03-03T00:00:00',
                                 u'_id': 3,
                                 u'author': u'tolstoy3'},
                                {u'published': None,
                                 u'_id': 4,
                                 u'author': u'tolstoy4'}]
        cls.expected_records23 = [{u'published': u'2005-03-03T00:00:00',
                                 u'_id': 3,
                                 u'author': u'tolstoy3'},
                                {u'published': None,
                                 u'_id': 4,
                                 u'author': u'tolstoy4'},
                                 {u'published': u'2005-03-05T00:00:00',
                                 u'_id': 5,
                                 u'author': u'tolstoy5'},
                                {u'published': None,
                                 u'_id': 6,
                                 u'author': u'tolstoy6'},
                                 {u'published': u'2005-03-05T00:00:00',
                                 u'_id': 7,
                                 u'author': u'tolstoy7'}]
        cls.expected_records3 = [{u'published': u'2005-03-05T00:00:00',
                                 u'_id': 5,
                                 u'author': u'tolstoy5'},
                                {u'published': None,
                                 u'_id': 6,
                                 u'author': u'tolstoy6'},
                                 {u'published': u'2005-03-05T00:00:00',
                                 u'_id': 7,
                                 u'author': u'tolstoy7'}]

        engine = db._get_engine(
                {'connection_url': pylons.config['ckan.datastore.write_url']}
            )
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

    @classmethod
    def teardown_class(cls):
        rebuild_all_dbs(cls.Session)
        p.unload('timeseries')

    def test_search_timeseries_fromtime(self):
        
        data = {'resource_id': self.data['resource_id'],
                'fromtime':'last 2s'
        }

        postparams = '%s=1' % json.dumps(data)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/datastore_ts_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        print(result)
        assert result['total'] <= len(self.data3['records'])
        assert result['records'] == self.expected_records3, result['records']

        data3456 = {'resource_id': self.data['resource_id'],
            'fromtime':'last 4s'
        }

        postparams = '%s=1' % json.dumps(data3456)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/datastore_ts_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] <= len(self.data2['records']) + len(self.data3['records'])
        assert result['records'] == self.expected_records23, result['records']

        data56 = {'resource_id': self.data['resource_id'],
            'fromtime':'last 6s'
        }

        postparams = '%s=1' % json.dumps(data56)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/datastore_ts_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] <= len(self.data['records']) + len(self.data2['records']) + len(self.data3['records'])
        assert result['records'] == self.expected_records, result['records']


class TestDatastore_TsSearchCompat(tests.WsgiAppCase):
    sysadmin_user = None
    normal_user = None

    @classmethod
    def setup_class(cls):
        if not tests.is_datastore_supported():
            raise nose.SkipTest("Datastore not supported")
        p.load('timeseries')
        helpers.reset_db()

        ctd.CreateTestData.create()
        cls.sysadmin_user = model.User.get('testsysadmin')
        cls.normal_user = model.User.get('annafan')
        cls.dataset = model.Package.get('annakarenina')
        cls.resource = cls.dataset.resources[0]
        cls.data = {
            'resource_id': cls.resource.id,
            'force': True,
            'aliases': 'books3',
            'fields': [{'id': 'author', 'type': 'text'},
                       {'id': 'published'}],
            'records': [{'author': 'tolstoy1',
                        'published': '2005-03-01'},
                        {'author': 'tolstoy2'}
                       ]
        }
        cls.data2 = {
            'resource_id': cls.resource.id,
            'force': True,
            'method': 'insert',
            'records': [{'author': 'tolstoy3',
                        'published': '2005-03-03'},
                        {'author': 'tolstoy4'}
                       ]
        }
        cls.data3 = {
            'resource_id': cls.resource.id,
            'force': True,
            'method': 'insert',
            'records': [{'author': 'tolstoy5',
                        'published': '2005-03-05'},
                        {'author': 'tolstoy6'},
                        {'author': 'tolstoy7',
                        'published': '2005-03-05'}
                       ]
        }
        cls.startdata = utcnow()
        postparams = '%s=1' % json.dumps(cls.data)
        auth = {'Authorization': str(cls.sysadmin_user.apikey)}
        res = cls.app.post('/api/action/datastore_ts_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True

        cls.enddata = utcnow()
        cls.startdata2 = utcnow()
        time.sleep(2)

        postparams = '%s=1' % json.dumps(cls.data2)
        res = cls.app.post('/api/action/datastore_ts_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True

        cls.enddata2 = utcnow()
        cls.startdata3 = utcnow()
        time.sleep(2)

        postparams = '%s=1' % json.dumps(cls.data3)
        res = cls.app.post('/api/action/datastore_ts_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True
        
        cls.enddata3 = utcnow()

        # Make an organization, because private datasets must belong to one.
        cls.organization = tests.call_action_api(
            cls.app, 'organization_create',
            name='test_org',
            apikey=cls.sysadmin_user.apikey)

        cls.expected_records = [{u'published': u'2005-03-01T00:00:00',
                                 u'_id': 1,
                                 u'author': u'tolstoy1'},
                                {u'published': None,
                                 u'_id': 2,
                                 u'author': u'tolstoy2'},
                                 {u'published': u'2005-03-03T00:00:00',
                                 u'_id': 3,
                                 u'author': u'tolstoy3'},
                                {u'published': None,
                                 u'_id': 4,
                                 u'author': u'tolstoy4'},
                                 {u'published': u'2005-03-05T00:00:00',
                                 u'_id': 5,
                                 u'author': u'tolstoy5'},
                                {u'published': None,
                                 u'_id': 6,
                                 u'author': u'tolstoy6'},
                                 {u'published': u'2005-03-05T00:00:00',
                                 u'_id': 7,
                                 u'author': u'tolstoy7'}]
        cls.expected_records1 = [{u'published': u'2005-03-01T00:00:00',
                                 u'_id': 1,
                                 u'author': u'tolstoy1'},
                                {u'published': None,
                                 u'_id': 2,
                                 u'author': u'tolstoy2'}]
        cls.expected_records12 = [{u'published': u'2005-03-01T00:00:00',
                                 u'_id': 1,
                                 u'author': u'tolstoy1'},
                                {u'published': None,
                                 u'_id': 2,
                                 u'author': u'tolstoy2'},
                                 {u'published': u'2005-03-03T00:00:00',
                                 u'_id': 3,
                                 u'author': u'tolstoy3'},
                                {u'published': None,
                                 u'_id': 4,
                                 u'author': u'tolstoy4'}]
        cls.expected_records23 = [{u'published': u'2005-03-03T00:00:00',
                                 u'_id': 3,
                                 u'author': u'tolstoy3'},
                                {u'published': None,
                                 u'_id': 4,
                                 u'author': u'tolstoy4'},
                                 {u'published': u'2005-03-05T00:00:00',
                                 u'_id': 5,
                                 u'author': u'tolstoy5'},
                                {u'published': None,
                                 u'_id': 6,
                                 u'author': u'tolstoy6'},
                                 {u'published': u'2005-03-05T00:00:00',
                                 u'_id': 7,
                                 u'author': u'tolstoy7'}]
        cls.expected_records3 = [{u'author': u'tolstoy5'},
                                 {u'author': u'tolstoy6'},
                                 {u'author': u'tolstoy7'}]

        engine = db._get_engine(
                {'connection_url': pylons.config['ckan.datastore.write_url']}
            )
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

    @classmethod
    def teardown_class(cls):
        rebuild_all_dbs(cls.Session)
        p.unload('timeseries')

    def test_search_timeseries_compat(self):
        
        data = {'resource_id': self.data['resource_id'],
            'fromtime':'last 2s',
            'fields': 'author'
        }

        postparams = '%s=1' % json.dumps(data)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/datastore_ts_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        print(result)
        print(self.expected_records3)
        assert result['total'] <= len(self.data3['records'])
        assert result['records'] == self.expected_records3, result['records']

        data3456 = {'resource_id': self.data['resource_id'],
            'fromtime':'last 4s'
        }

        postparams = '%s=1' % json.dumps(data3456)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/datastore_ts_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] <= len(self.data2['records']) + len(self.data3['records'])
        assert result['records'] == self.expected_records23, result['records']

        data56 = {'resource_id': self.data['resource_id'],
            'fromtime':'last 6s'
        }

        postparams = '%s=1' % json.dumps(data56)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/datastore_ts_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] <= len(self.data['records']) + len(self.data2['records']) + len(self.data3['records'])
        assert result['records'] == self.expected_records, result['records']
