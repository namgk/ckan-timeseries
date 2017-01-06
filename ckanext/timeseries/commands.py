from __future__ import print_function
import argparse
import os
import sys

import ckan.lib.cli as cli
import ckanext.timeseries.db as db
import sqlalchemy.orm as orm
from sqlalchemy import create_engine

def _abort(message):
    print(message, file=sys.stderr)
    sys.exit(1)

def _migrate_autogen_timestamp(old_name, new_name):
    write_url_obj = cli.parse_db_config('ckan.datastore.write_url')

    write_url = 'postgres://'+ write_url_obj['db_user'] + ':'
    write_url = write_url + write_url_obj['db_pass'] + '@'
    write_url = write_url + write_url_obj['db_host']
    write_url = write_url + (write_url_obj['db_port'] if write_url_obj['db_port'] else '') + '/'
    write_url = write_url + write_url_obj['db_name']

    conn = create_engine(write_url)
    
    sql_autogen_res = 'select table_name \
        from INFORMATION_SCHEMA.COLUMNS where column_name = %s'
    sql_rename_column = 'ALTER TABLE "{table_name}" RENAME {old_name} TO {new_name}'

    autogen_res = conn.execute(sql_autogen_res, old_name).fetchall()
    for ar in autogen_res:
        result = conn.execute(sql_rename_column.format(
            table_name = ar[0],
            old_name = old_name,
            new_name = new_name))

def _set_permissions(args):
    write_url = cli.parse_db_config('ckan.datastore.write_url')
    read_url = cli.parse_db_config('ckan.datastore.read_url')
    db_url = cli.parse_db_config('sqlalchemy.url')

    # Basic validation that read and write URLs reference the same database.
    # This obviously doesn't check they're the same database (the hosts/ports
    # could be different), but it's better than nothing, I guess.
    if write_url['db_name'] != read_url['db_name']:
        _abort("The datastore write_url and read_url must refer to the same "
               "database!")

    context = {
        'maindb': db_url['db_name'],
        'datastoredb': write_url['db_name'],
        'mainuser': db_url['db_user'],
        'writeuser': write_url['db_user'],
        'readuser': read_url['db_user'],
    }

    sql = _permissions_sql(context)

    print(sql)


def _permissions_sql(context):
    template_filename = os.path.join(os.path.dirname(__file__),
                                     'set_permissions.sql')
    with open(template_filename) as fp:
        template = fp.read()
    return template.format(**context)


parser = argparse.ArgumentParser(
    prog='paster timeseries',
    description='Perform commands to set up the datastore',
    epilog='Make sure that the datastore URLs are set properly before you run '
           'these commands!')
subparsers = parser.add_subparsers(title='commands')

# parser_set_perms = subparsers.add_parser(
#     'set-permissions',
#     description='Set the permissions on the datastore.',
#     help='This command will help ensure that the permissions for the '
#          'datastore users as configured in your configuration file are '
#          'correct at the database. It will emit an SQL script that '
#          'you can use to set these permissions.',
#     epilog='"The ships hung in the sky in much the same way that bricks '
#            'don\'t."')
# parser_set_perms.set_defaults(func=_set_permissions)

parser_upgrade_schema = subparsers.add_parser(
    'upgrade-schema',
    description='Upgrade schema from v0.0.3 to v0.1.0',
    help='This command is used to migrade schema of CKAN Timeseries API'
         'Prior to v0.1.0, all resource tables have a column name autogen_timestamp'
         'However, since v0.1.0, this column has been renamed to _autogen_timestamp'
         'This change is to make it conform with the private/public field naming scheme',
    epilog='"Be careful, better backup your db first!!!"')
parser_upgrade_schema.add_argument('old_name', type=str, help='old column name')
parser_upgrade_schema.add_argument('new_name', type=str, help='new column name')
parser_upgrade_schema.set_defaults(func=_migrate_autogen_timestamp)


class SetupTimeseriesCommand(cli.CkanCommand):
    summary = parser.description

    def command(self):
        self._load_config()

        args = parser.parse_args(self.args)
        old_name = args.old_name
        new_name = args.new_name
        args.func(old_name,new_name)
