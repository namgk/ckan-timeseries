import logging
import ckan.plugins as p

log = logging.getLogger(__name__)

def before_create(context, resource):
  log.debug('before create ................' )
  resource['url'] = '_datastore_only_resource'
  log.debug('{}'.format(resource))


def after_create(context, resource):
  log.debug('after create ................' )
  resource['url_type'] = 'datastore'
  p.toolkit.get_action('resource_update')(context, resource)

  log.debug('{}'.format(resource))