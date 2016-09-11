import json
import nose
import datetime

import pylons
import sqlalchemy.orm as orm

import ckan.plugins as p
import ckan.lib.create_test_data as ctd
import ckan.model as model
import ckan.tests.legacy as tests

import ckanext.datastore.db as db
from ckanext.datastore.tests.helpers import rebuild_all_dbs, set_url_type

assert_equal = nose.tools.assert_equal


class TestDatastoreUpsert(tests.WsgiAppCase):
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
        set_url_type(
            model.Package.get('annakarenina').resources, cls.sysadmin_user)
        resource = model.Package.get('annakarenina').resources[0]
        cls.data = {
            'resource_id': resource.id,
            'fields': [{'id': u'b\xfck', 'type': 'text'},
                       {'id': 'author', 'type': 'text'},
                       {'id': 'nested', 'type': 'json'},
                       {'id': 'characters', 'type': 'text[]'},
                       {'id': 'published'}],
            'primary_key': u'b\xfck',
            'records': [{u'b\xfck': 'annakarenina', 'author': 'tolstoy',
                        'published': '2005-03-01', 'nested': ['b', {'moo': 'moo'}]},
                        {u'b\xfck': 'warandpeace', 'author': 'tolstoy',
                        'nested': {'a':'b'}}
                       ]
            }
        postparams = '%s=1' % json.dumps(cls.data)
        auth = {'Authorization': str(cls.sysadmin_user.apikey)}
        res = cls.app.post('/api/action/datastore_ts_create', params=postparams,
                           extra_environ=auth)
        res_dict = json.loads(res.body)
        assert res_dict['success'] is True

        engine = db._get_engine(
            {'connection_url': pylons.config['ckan.datastore.write_url']})
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))


    @classmethod
    def teardown_class(cls):
        rebuild_all_dbs(cls.Session)
        p.unload('datastore_ts')


    def test_insert_timeseries(self):
        hhguide = u"hitchhiker's guide to the galaxy"
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'insert',
            'records': [{
                'author': 'adams',
                'characters': ['Arthur Dent', 'Marvin'],
                'nested': {'foo': 'bar', 'baz': 3},
                u'b\xfck': hhguide}]
        }

        postparams = '%s=1' % json.dumps(data)
        auth = {'Authorization': str(self.sysadmin_user.apikey)}
        res = self.app.post('/api/action/datastore_ts_upsert', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        assert res_dict['success'] is True

        c = self.Session.connection()
        results = c.execute('select * from "{0}"'.format(self.data['resource_id']))
        self.Session.remove()

        for row in results:
            assert 'autogen_timestamp' in row

        assert results.rowcount == 3