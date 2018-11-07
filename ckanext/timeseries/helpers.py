# encoding: utf-8

import json
import logging

import paste.deploy.converters as converters
import sqlparse
import datetime
import pytz
import iso8601
import re
from ckan.common import config
from six import string_types

from ckan.plugins.toolkit import get_action, ObjectNotFound, NotAuthorized

log = logging.getLogger(__name__)

class Timeseries_query(object):
    def __init__(self, s):
        self.d = 0
        self.h = 0
        self.m = 0
        self.s = 0
        self.elm_pattern = re.compile('[0-9]+[d|h|m|s]')
        self.parse(s)

    def text(self):
        return (self.d, self.h, self.m, self.s)

    def parse(self,s):
        # str: 1d,2h,3m,4s
        query = s.split(',')
        for elm in query:
            print(elm)
            if not self.elm_pattern.match(elm):
                raise ValueError('Cannot parse query: {}'.format(elm))
            
            for e in ['d', 'h', 'm', 's']:
                if e in elm:
                    setattr(self, e, int(elm.split(e)[0]))

def is_single_statement(sql):
    '''Returns True if received SQL string contains at most one statement'''
    return len(sqlparse.split(sql)) <= 1


def is_valid_field_name(name):
    '''
    Check that field name is valid:
    * can't start or end with whitespace characters
    * can't start with underscore
    * can't contain double quote (")
    * can't be empty
    '''
    return (name and name == name.strip() and
            not name.startswith('_') and
            '"' not in name)


def is_valid_table_name(name):
    if '%' in name:
        return False
    return is_valid_field_name(name)


def get_list(input, strip_values=True):
    '''Transforms a string or list to a list'''
    if input is None:
        return
    if input == '':
        return []

    converters_list = converters.aslist(input, ',', True)
    if strip_values:
        return [_strip(x) for x in converters_list]
    else:
        return converters_list


def validate_int(i, non_negative=False):
    try:
        i = int(i)
    except ValueError:
        return False
    return i >= 0 or not non_negative


def _strip(s):
    if isinstance(s, string_types) and len(s) and s[0] == s[-1]:
        return s.strip().strip('"')
    return s


def should_fts_index_field_type(field_type):
    return field_type.lower() in ['tsvector', 'text', 'number']


def get_table_names_from_sql(context, sql):
    '''Parses the output of EXPLAIN (FORMAT JSON) looking for table names

    It performs an EXPLAIN query against the provided SQL, and parses
    the output recusively looking for "Relation Name".

    Note that this requires Postgres 9.x.

    :param context: a CKAN context dict. It must contain a 'connection' key
        with the current DB connection.
    :type context: dict
    :param sql: the SQL statement to parse for table names
    :type sql: string

    :rtype: list of strings
    '''

    queries = [sql]
    table_names = []

    while queries:
        sql = queries.pop()
        result = context['connection'].execute(
            'EXPLAIN (VERBOSE, FORMAT JSON) {0}'.format(
                sql.encode('utf-8'))).fetchone()

        try:
            query_plan = json.loads(result['QUERY PLAN'])
            plan = query_plan[0]['Plan']

            t, q = _get_table_names_queries_from_plan(plan)
            table_names.extend(t)
            queries.extend(q)

        except ValueError:
            log.error('Could not parse query plan')
            raise

    return table_names


def _get_table_names_queries_from_plan(plan):

    table_names = []
    queries = []

    if plan.get('Relation Name'):
        table_names.append(plan['Relation Name'])

    if 'Function Name' in plan and plan['Function Name'].startswith(
            'crosstab'):
        try:
            queries.append(_get_subquery_from_crosstab_call(
                plan['Function Call']))
        except ValueError:
            table_names.append('_unknown_crosstab_sql')

    if 'Plans' in plan:
        for child_plan in plan['Plans']:
            t, q = _get_table_names_queries_from_plan(child_plan)
            table_names.extend(t)
            queries.extend(q)

    return table_names, queries


def _get_subquery_from_crosstab_call(ct):
    """
    Crosstabs are a useful feature some sites choose to enable on
    their datastore databases. To support the sql parameter passed
    safely we accept only the simple crosstab(text) form where text
    is a literal SQL string, otherwise raise ValueError
    """
    if not ct.startswith("crosstab('") or not ct.endswith("'::text)"):
        raise ValueError('only simple crosstab calls supported')
    ct = ct[10:-8]
    if "'" in ct.replace("''", ""):
        raise ValueError('only escaped single quotes allowed in query')
    return ct.replace("''", "'")


def datastore_dictionary(resource_id):
    """
    Return the data dictionary info for a resource
    """
    try:
        return [
            f for f in get_action('timeseries_search')(
                None, {
                    u'resource_id': resource_id,
                    u'limit': 0,
                    u'include_total': False})['fields']
            if not f['id'].startswith(u'_')]
    except (ObjectNotFound, NotAuthorized):
        return []

def dict_rm_autogen_timestamp(dict):
    if u'_autogen_timestamp' in dict:
        dict.pop(u'_autogen_timestamp', None)
    return dict

def remove_autogen(result):
    if 'fields' in result:
        result['fields'] = [f for f in result['fields'] if f.get('id') != u'_autogen_timestamp']
    if 'records' in result:
        result['records'] = map(dict_rm_autogen_timestamp, result['records'])

def timestamp_from_string(str):
    print(str)
    if (str.startswith( 'last ', 0, 5 )):
        # sample queries: 1m; 1m,2s; 1d,2h,3m,4s
        query = Timeseries_query(str.split('last ')[1])
        diff = datetime.timedelta(seconds=query.s, minutes=query.m, hours=query.h, days=query.d)
        return utcnow() - diff
        
    return iso8601.parse_date(str)
        
def string_from_timestamp(timestamp):
    return timestamp.isoformat()

def utcnow():
    return datetime.datetime.now(tz=pytz.utc)

def get_max_resource_size():
    # get allowed table size configuration, default to 500MB
    # the number in configuration file is in MB
    try:
        max_resource_size = int(config.get('ckan.timeseries.max_resource_size'))
    except ValueError as err:
        max_resource_size = 500 # in MB
    except TypeError as err:
        max_resource_size = 500 # in MB
        
    return max_resource_size * 1000 * 1000
