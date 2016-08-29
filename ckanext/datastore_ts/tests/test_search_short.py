import json
import nose
import pprint

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


class TestDatastoreSearchNewTest(object):
    @classmethod
    def setup_class(cls):
        p.load('datastore_ts')

    @classmethod
    def teardown_class(cls):
        p.unload('datastore_ts')
        helpers.reset_db()

    def test_fts_on_field_calculates_ranks_only_on_that_specific_field(self):
        resource = factories.Resource()
        data = {
            'resource_id': resource['id'],
            'force': True,
            'records': [
                {'from': 'Brazil', 'to': 'Brazil'},
                {'from': 'Brazil', 'to': 'Italy'}
            ],
        }
        result = helpers.call_action('datastore_ts_create', **data)
        search_data = {
            'resource_id': resource['id'],
            'fields': 'from',
            'q': {
                'from': 'Brazil'
            },
        }
        result = helpers.call_action('datastore_ts_search', **search_data)
        ranks = [r['rank from'] for r in result['records']]
        assert_equals(len(result['records']), 2)
        assert_equals(len(set(ranks)), 1)

    def test_fts_works_on_non_textual_fields(self):
        resource = factories.Resource()
        data = {
            'resource_id': resource['id'],
            'force': True,
            'records': [
                {'from': 'Brazil', 'year': {'foo': 2014}},
                {'from': 'Brazil', 'year': {'foo': 1986}}
            ],
        }
        result = helpers.call_action('datastore_ts_create', **data)
        search_data = {
            'resource_id': resource['id'],
            'fields': 'year',
            'plain': False,
            'q': {
                'year': '20:*'
            },
        }
        result = helpers.call_action('datastore_ts_search', **search_data)
        assert_equals(len(result['records']), 1)
        assert_equals(result['records'][0]['year'], {'foo': 2014})

    def test_all_params_work_with_fields_with_whitespaces(self):
        resource = factories.Resource()
        data = {
            'resource_id': resource['id'],
            'force': True,
            'records': [
                {'the year': 2014},
                {'the year': 2013},
            ],
        }
        result = helpers.call_action('datastore_ts_create', **data)
        search_data = {
            'resource_id': resource['id'],
            'fields': 'the year',
            'sort': 'the year',
            'filters': {
                'the year': 2013
            },
            'q': {
                'the year': '2013'
            },
        }
        result = helpers.call_action('datastore_ts_search', **search_data)
        result_years = [r['the year'] for r in result['records']]
        assert_equals(result_years, [2013])



class TestDatastoreSearch(tests.WsgiAppCase):
    sysadmin_user = None
    normal_user = None

    @classmethod
    def setup_class(cls):
        if not tests.is_datastore_supported():
            raise nose.SkipTest("Datastore not supported")
        p.load('datastore_ts')
        ctd.CreateTestData.create()
        cls.sysadmin_user = model.User.get('testsysadmin')
        cls.normal_user = model.User.get('annafan')
        cls.dataset = model.Package.get('annakarenina')
        cls.resource = cls.dataset.resources[0]
        cls.data = {
            'resource_id': cls.resource.id,
            'force': True,
            'aliases': 'books3',
            'fields': [{'id': u'b\xfck', 'type': 'text'},
                       {'id': 'author', 'type': 'text'},
                       {'id': 'published'},
                       {'id': u'characters', u'type': u'_text'},
                       {'id': 'rating with %'}],
            'records': [{u'b\xfck': 'annakarenina', 'author': 'tolstoy',
                        'published': '2005-03-01', 'nested': ['b', {'moo': 'moo'}],
                        u'characters': [u'Princess Anna', u'Sergius'],
                        'rating with %': '60%'},
                        {u'b\xfck': 'warandpeace', 'author': 'tolstoy',
                        'nested': {'a': 'b'}, 'rating with %': '99%'}
                       ]
        }
        postparams = '%s=1' % json.dumps(cls.data)
        auth = {'Authorization': str(cls.sysadmin_user.apikey)}
        res = cls.app.post('/api/action/datastore_ts_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True

        # Make an organization, because private datasets must belong to one.
        cls.organization = tests.call_action_api(
            cls.app, 'organization_create',
            name='test_org',
            apikey=cls.sysadmin_user.apikey)

        cls.expected_records = [{u'published': u'2005-03-01T00:00:00',
                                 u'_id': 1,
                                 u'nested': [u'b', {u'moo': u'moo'}],
                                 u'b\xfck': u'annakarenina',
                                 u'author': u'tolstoy',
                                 u'characters': [u'Princess Anna', u'Sergius'],
                                 u'rating with %': u'60%'},
                                {u'published': None,
                                 u'_id': 2,
                                 u'nested': {u'a': u'b'},
                                 u'b\xfck': u'warandpeace',
                                 u'author': u'tolstoy',
                                 u'characters': None,
                                 u'rating with %': u'99%'}]

        engine = db._get_engine(
                {'connection_url': pylons.config['ckan.datastore.write_url']}
            )
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

    @classmethod
    def teardown_class(cls):
        rebuild_all_dbs(cls.Session)
        p.unload('datastore_ts')

    def test_search_sort(self):
        data = {'resource_id': self.data['resource_id'],
                'sort': u'b\xfck asc, author desc'}
        postparams = '%s=1' % json.dumps(data)
        auth = {'Authorization': str(self.normal_user.apikey)}
        res = self.app.post('/api/action/datastore_ts_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] == 2

        assert result['records'] == self.expected_records, result['records']

        data = {'resource_id': self.data['resource_id'],
                'sort': [u'b\xfck desc', '"author" asc']}
        postparams = '%s=1' % json.dumps(data)
        res = self.app.post('/api/action/datastore_ts_search', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True
        result = res_dict['result']
        assert result['total'] == 2

        assert result['records'] == self.expected_records[::-1]
