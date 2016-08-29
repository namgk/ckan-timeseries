import json
import nose
import sys
from nose.tools import assert_equal, raises

import pylons
from pylons import config
import sqlalchemy.orm as orm
import paste.fixture

import ckan.plugins as p
import ckan.lib.create_test_data as ctd
import ckan.model as model
import ckan.tests.legacy as tests
import ckan.config.middleware as middleware
import ckan.tests.helpers as helpers
import ckan.tests.factories as factories

import ckanext.datastore_ts.db as db
from ckanext.datastore_ts.tests.helpers import rebuild_all_dbs, set_url_type


# avoid hanging tests https://github.com/gabrielfalcao/HTTPretty/issues/34
if sys.version_info < (2, 7, 0):
    import socket
    socket.setdefaulttimeout(1)

class TestDatastoreCreate(tests.WsgiAppCase):
    sysadmin_user = None
    normal_user = None

    @classmethod
    def setup_class(cls):

        wsgiapp = middleware.make_app(config['global_conf'], **config)
        cls.app = paste.fixture.TestApp(wsgiapp)
        if not tests.is_datastore_supported():
            raise nose.SkipTest("Datastore not supported")
        p.load('datastore_ts')
        ctd.CreateTestData.create()
        cls.sysadmin_user = model.User.get('testsysadmin')
        cls.normal_user = model.User.get('annafan')
        engine = db._get_engine(
            {'connection_url': pylons.config['ckan.datastore.write_url']})
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))
        set_url_type(
            model.Package.get('annakarenina').resources, cls.sysadmin_user)

    @classmethod
    def teardown_class(cls):
        rebuild_all_dbs(cls.Session)
        p.unload('datastore_ts')

    def test_guess_types(self):
        resource = model.Package.get('annakarenina').resources[1]

        data = {
            'resource_id': resource.id
        }
        postparams = '%s=1' % json.dumps(data)
        auth = {'Authorization': str(self.sysadmin_user.apikey)}
        res = self.app.post('/api/action/datastore_ts_delete', params=postparams,
                            extra_environ=auth, status="*")  # ignore status
        res_dict = json.loads(res.body)

        data = {
            'resource_id': resource.id,
            'fields': [{'id': 'author', 'type': 'json'},
                       {'id': 'count'},
                       {'id': 'book'},
                       {'id': 'date'}],
            'records': [{'book': 'annakarenina', 'author': 'tolstoy',
                         'count': 1, 'date': '2005-12-01', 'count2': 0.5},
                        {'book': 'crime', 'author': ['tolstoy', 'dostoevsky']},
                        {'book': 'warandpeace'}]  # treat author as null
        }
        postparams = '%s=1' % json.dumps(data)
        auth = {'Authorization': str(self.sysadmin_user.apikey)}
        res = self.app.post('/api/action/datastore_ts_create', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        c = self.Session.connection()
        results = c.execute('''select * from "{0}" '''.format(resource.id))

        types = [db._pg_types[field[1]] for field in results.cursor.description]
        print(types)

        assert types == [u'int4', u'tsvector', u'float8', u'nested', u'int4', u'text', u'timestamp', u'float8'], types
                                               # autogen_timestamp
        assert results.rowcount == 3
        for i, row in enumerate(results):
            assert data['records'][i].get('book') == row['book']
            assert data['records'][i].get('author') == (
                json.loads(row['author'][0]) if row['author'] else None)
        self.Session.remove()

        ### extend types

        data = {
            'resource_id': resource.id,
            'fields': [{'id': 'author', 'type': 'text'},
                       {'id': 'count'},
                       {'id': 'book'},
                       {'id': 'date'},
                       {'id': 'count2'},
                       {'id': 'extra', 'type':'text'},
                       {'id': 'date2'},
                      ],
            'records': [{'book': 'annakarenina', 'author': 'tolstoy',
                         'count': 1, 'date': '2005-12-01', 'count2': 2,
                         'nested': [1, 2], 'date2': '2005-12-01'}]
        }

        postparams = '%s=1' % json.dumps(data)
        auth = {'Authorization': str(self.sysadmin_user.apikey)}
        res = self.app.post('/api/action/datastore_ts_create', params=postparams,
                            extra_environ=auth)
        res_dict = json.loads(res.body)

        c = self.Session.connection()
        results = c.execute('''select * from "{0}" '''.format(resource.id))
        self.Session.remove()

        types = [db._pg_types[field[1]] for field in results.cursor.description]
        print(types)
        assert types == [u'int4',  # id
                         u'tsvector',  # fulltext
                         u'float8', # autogen_timestamp
                         u'nested',  # author
                         u'int4',  # count
                         u'text',  # book
                         u'timestamp',  # date
                         u'float8',  # count2
                         u'text',  # extra
                         u'timestamp',  # date2
                         u'nested',  # count3
                        ], types
