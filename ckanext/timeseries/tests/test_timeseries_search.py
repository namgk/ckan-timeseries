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
    extract, rebuild_all_dbs,
    DatastoreFunctionalTestBase, DatastoreLegacyTestBase)

import ckan.tests.helpers as helpers
import ckan.tests.factories as factories

assert_equals = nose.tools.assert_equals
assert_raises = nose.tools.assert_raises
assert_in = nose.tools.assert_in


class TestTimeseriesSearch(DatastoreLegacyTestBase):
    sysadmin_user = None
    normal_user = None

    @classmethod
    def setup_class(cls):
        cls.app = helpers._get_test_app()
        super(TestTimeseriesSearch, cls).setup_class()
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
        res = cls.app.post('/api/action/timeseries_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True

        
        cls.enddata = utcnow()
        cls.startdata2 = utcnow()
        time.sleep(2)
        

        postparams = '%s=1' % json.dumps(cls.data2)
        res = cls.app.post('/api/action/timeseries_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True

        
        cls.enddata2 = utcnow()
        cls.startdata3 = utcnow()
        time.sleep(2)

        postparams = '%s=1' % json.dumps(cls.data3)
        res = cls.app.post('/api/action/timeseries_create', params=postparams,
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

        engine = db.get_write_engine()
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

    def test_search_timeseries(self):
        fromtime_str = self.startdata.isoformat().replace("+","%2B")
        totime_str = self.enddata2.isoformat().replace("+","%2B")

        data12 = {'resource_id': self.data['resource_id'],
                'fromtime':fromtime_str,
                'totime':totime_str
                # 'totime':str(self.enddata3)
        }

        postparams = '%s=1' % json.dumps(data12)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)


        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] == len(self.data['records']) + len(self.data2['records'])
        assert result['records'] == self.expected_records12, result['records']

        fromtime_str23 = self.startdata2.isoformat().replace("+","%2B")
        totime_str23 = self.enddata3.isoformat().replace("+","%2B")
        
        data23 = {'resource_id': self.data['resource_id'],
                'fromtime':fromtime_str23,
                'totime':totime_str23
        }

        postparams = '%s=1' % json.dumps(data23)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] == len(self.data2['records']) + len(self.data3['records'])
        assert result['records'] == self.expected_records23, result['records']

        data23 = {'resource_id': self.data['resource_id'],
                'fromtime':fromtime_str23,
                'totime':totime_str23
        }

        postparams = '%s=1' % json.dumps(data23)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] == len(self.data2['records']) + len(self.data3['records'])
        assert result['records'] == self.expected_records23, result['records']

    def test_search_timeseries_fromtime(self):
        fromtime_str = self.startdata.isoformat().replace("+","%2B")
        
        data = {'resource_id': self.data['resource_id'],
                'fromtime':fromtime_str
        }

        postparams = '%s=1' % json.dumps(data)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] == len(self.data['records']) + len(self.data2['records']) + len(self.data3['records'])
        assert result['records'] == self.expected_records, result['records']


        fromtime_str2 = self.startdata2.isoformat().replace("+","%2B")
        
        data3456 = {'resource_id': self.data['resource_id'],
                'fromtime':fromtime_str2
        }

        postparams = '%s=1' % json.dumps(data3456)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] == len(self.data2['records']) + len(self.data3['records'])
        assert result['records'] == self.expected_records23, result['records']


        fromtime_str3 = self.startdata3.isoformat().replace("+","%2B")
        
        data56 = {'resource_id': self.data['resource_id'],
                'fromtime':fromtime_str3
        }

        postparams = '%s=1' % json.dumps(data56)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] == len(self.data3['records'])
        assert result['records'] == self.expected_records3, result['records']

    def test_search_timeseries_totime(self):
        totime_str = self.enddata.isoformat().replace("+","%2B")
        
        data12 = {'resource_id': self.data['resource_id'],
                'totime':totime_str
        }

        postparams = '%s=1' % json.dumps(data12)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] == len(self.data['records'])
        assert result['records'] == self.expected_records1, result['records']



        totime_str4 = self.enddata2.isoformat().replace("+","%2B")
        
        data1234 = {'resource_id': self.data['resource_id'],
                'totime':totime_str4
        }

        postparams = '%s=1' % json.dumps(data1234)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] == len(self.data['records']) + len(self.data2['records'])
        assert result['records'] == self.expected_records12, result['records']


        totime_str6 = self.enddata3.isoformat().replace("+","%2B")
        
        data123456 = {'resource_id': self.data['resource_id'],
                'totime':totime_str6
        }

        postparams = '%s=1' % json.dumps(data123456)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] == len(self.data['records']) + len(self.data2['records']) + len(self.data3['records'])
        assert result['records'] == self.expected_records, result['records']

    def test_search_timeseries_fromtime_last(self):
        
        data = {'resource_id': self.data['resource_id'],
                'fromtime':'last 2s'
        }

        postparams = '%s=1' % json.dumps(data)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
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
        res = self.app.post('/api/action/timeseries_search', params=postparams,
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
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] <= len(self.data['records']) + len(self.data2['records']) + len(self.data3['records'])
        assert result['records'] == self.expected_records, result['records'] 

class TestTimeseriesSearchBad(DatastoreLegacyTestBase):
    sysadmin_user = None
    normal_user = None

    @classmethod
    def setup_class(cls):
        cls.app = helpers._get_test_app()
        super(TestTimeseriesSearchBad, cls).setup_class()
        ctd.CreateTestData.create()
        cls.sysadmin_user = model.User.get('testsysadmin')
        cls.normal_user = model.User.get('annafan')
        cls.dataset = model.Package.get('annakarenina')
        cls.resource = cls.dataset.resources[0]
        cls.data = {
            'resource_id': cls.resource.id,
            'force': True,
        }

        postparams = '%s=1' % json.dumps(cls.data)
        auth = {'Authorization': str(cls.sysadmin_user.apikey)}
        res = cls.app.post('/api/action/timeseries_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True

        engine = db.get_write_engine()
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

    def test_search_timeseries(self):
        # sample time string: 2016-10-02T03:40:21.019793+00:00
        fromtime_str = "2016-10q2"

        data1 = {'resource_id': self.data['resource_id'],
                'fromtime':fromtime_str
        }
        postparams = '%s=1' % json.dumps(data1)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth, status=409)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is False

        totime_str = "2016-30-03"

        data2 = {'resource_id': self.data['resource_id'],
                'totime':totime_str
        }
        postparams = '%s=1' % json.dumps(data2)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth, status=409)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is False


class TestTimeseriesSearchCompat(DatastoreLegacyTestBase):
    sysadmin_user = None
    normal_user = None

    @classmethod
    def setup_class(cls):
        cls.app = helpers._get_test_app()
        super(TestTimeseriesSearchCompat, cls).setup_class()
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
        res = cls.app.post('/api/action/timeseries_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True

        cls.enddata = utcnow()
        cls.startdata2 = utcnow()
        time.sleep(2)

        postparams = '%s=1' % json.dumps(cls.data2)
        res = cls.app.post('/api/action/timeseries_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True

        cls.enddata2 = utcnow()
        cls.startdata3 = utcnow()
        time.sleep(2)

        postparams = '%s=1' % json.dumps(cls.data3)
        res = cls.app.post('/api/action/timeseries_create', params=postparams,
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

        engine = db.get_write_engine()
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

    def test_search_timeseries_compat(self):
        
        data = {'resource_id': self.data['resource_id'],
            'fromtime':'last 2s',
            'fields': 'author'
        }

        postparams = '%s=1' % json.dumps(data)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/timeseries_search', params=postparams,
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
        res = self.app.post('/api/action/timeseries_search', params=postparams,
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
        res = self.app.post('/api/action/timeseries_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] <= len(self.data['records']) + len(self.data2['records']) + len(self.data3['records'])
        assert result['records'] == self.expected_records, result['records']
