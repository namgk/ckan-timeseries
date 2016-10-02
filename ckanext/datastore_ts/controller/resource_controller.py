import logging
import pylons

import ckan.plugins as p
import ckan.logic as logic

import ckanext.datastore_ts.db as db

log = logging.getLogger(__name__)
_get_or_bust = logic.get_or_bust
def before_create(context, resource):
  log.debug('before create ................' )
  # {'description': u'', 'format': u'', 'url': u'1', 
  #   'package_id': u'testset', 'name': u'1'}
  # resource['url'] = '_datastore_only_resource'
  # log.debug('{}'.format(resource))


def after_create(context, resource):
  log.debug('after create ................' )
  # {u'cache_last_updated': None, 
  #   u'package_id': u'2b62a518-05b8-46d5-a3b9-8bc43a2a8510', 
  #   u'webstore_last_updated': None, 
  #   u'datastore_active': False, 
  #   u'id': u'239a80ef-57d1-43e9-8dd6-3f2c08955002', 
  #   u'size': None, u'state': u'active', u'hash': u'', 
  #   u'description': u'', u'format': u'', 
  #   u'mimetype_inner': None, u'url_type': None, 
  #   u'mimetype': None, u'cache_url': None, 
  #   u'name': u'1', u'created': '2016-09-25T21:33:04.592684', 
  #   u'url': u'http://1', u'webstore_url': None, 
  #   u'last_modified': None, u'position': 3, 
  #   u'revision_id': u'4d28cba7-b49e-413f-95e3-e9332e60f57a', 
  #   u'resource_type': None}
  # resource['url_type'] = 'datastore'
  # resource['datastore_active'] = True
  # p.toolkit.get_action('resource_update')(context, resource)

  # data_dict = {'resource': resource, 'resource_id': resource['id']}
  # data_dict['connection_url'] = pylons.config['ckan.datastore.write_url']

  # model = _get_or_bust(context, 'model')
  # resource = model.Resource.get(data_dict['resource_id'])
  # legacy_mode = 'ckan.datastore.read_url' not in pylons.config
  # if not legacy_mode and resource.package.private:
  #   data_dict['private'] = True

  # result = db.create(context, data_dict)
  # log.debug('{}'.format(result))