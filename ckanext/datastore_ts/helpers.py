import logging
import json
import datetime
import pytz
import sqlparse
import iso8601
from iso8601 import ParseError
import ckan.plugins.toolkit as toolkit
import paste.deploy.converters as converters
import re
        

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

def dict_rm_autogen_timestamp(dict):
    if u'autogen_timestamp' in dict:
        dict.pop(u'autogen_timestamp', None)
    return dict

def remove_autogen(result):
    if 'fields' in result:
        result['fields'] = [f for f in result['fields'] if f.get('id') != u'autogen_timestamp']
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

def get_list(input, strip_values=True):
    '''Transforms a string or list to a list'''
    if input is None:
        return
    if input == '':
        return []

    l = converters.aslist(input, ',', True)
    if strip_values:
        return [_strip(x) for x in l]
    else:
        return l


def is_single_statement(sql):
    '''Returns True if received SQL string contains at most one statement'''
    return len(sqlparse.split(sql)) <= 1


def validate_int(i, non_negative=False):
    try:
        i = int(i)
    except ValueError:
        return False
    return i >= 0 or not non_negative


def _strip(input):
    if isinstance(input, basestring) and len(input) and input[0] == input[-1]:
        return input.strip().strip('"')
    return input


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

    def _get_table_names_from_plan(plan):

        table_names = []

        if plan.get('Relation Name'):
            table_names.append(plan['Relation Name'])

        if 'Plans' in plan:
            for child_plan in plan['Plans']:
                table_name = _get_table_names_from_plan(child_plan)
                if table_name:
                    table_names.extend(table_name)

        return table_names

    result = context['connection'].execute(
        'EXPLAIN (FORMAT JSON) {0}'.format(sql.encode('utf-8'))).fetchone()

    table_names = []

    try:
        query_plan = json.loads(result['QUERY PLAN'])
        plan = query_plan[0]['Plan']

        table_names.extend(_get_table_names_from_plan(plan))

    except ValueError:
        log.error('Could not parse query plan')

    return table_names


def literal_string(s):
    """
    Return s as a postgres literal string
    """
    return u"'" + s.replace(u"'", u"''").replace(u'\0', '') + u"'"


def identifier(s):
    """
    Return s as a quoted postgres identifier
    """
    return u'"' + s.replace(u'"', u'""').replace(u'\0', '') + u'"'
