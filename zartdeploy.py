'Deploy tool'
from __future__ import print_function
import os
import sys
import argparse
from subprocess import Popen, PIPE

try:
    from urllib.parse import urlunsplit  # Python 3
except ImportError:
    from urlparse import urlunsplit  # Python 2

version = '0.1.0'


def _clean(x):
    "poorman's dedent and whitespace cleaner"
    return ' '.join(x.split())


# LocalDB constants
LOCALDB_SQL_URL = 'mssql://(localdb)\\{instance}/{database}'
LOCALDB_SYSTEM_DATABASES = ['master', 'model', 'tempdb', 'msdb']
LOCALDB_CREATE_DATABASE = _clean('CREATE DATABASE [{database}]')
LOCALDB_CREATE_DATABASE_ON = _clean(
    '''\
CREATE DATABASE [{database}]
ON (NAME='{database}dev', FILENAME='{path}{database}.mdf')
LOG ON (NAME='{database}log', FILENAME='{path}{database}.ldf')'''
)
LOCALDB_DROP_DATABASE = _clean(
    '''\
IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = N'{database}')
DROP DATABASE [{database}]'''
)

# simple version
# LOCALDB_DROP_DATABASE = _clean('DROP DATABASE [{database}]')

# SQL2016+ version
# LOCALDB_DROP_DATABASE = _clean('DROP DATABASE IF EXISTS [{database}]')

# Complex version. Gives errors due to USE, so scrapped for now
# LOCALDB_DROP_DATABASE = _clean('''\
# USE [tempdb];
# IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = N'{database}')
# BEGIN
#  USE [{database}];
#  ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
#  USE [tempdb];
#  DROP DATABASE [{database}]
# END''')


def _quote(args):
    'quote args for output'
    return ' '.join('"%s"' % arg if ' ' in arg else arg for arg in args)


# TODO: fix logging
def _run(*args, **kw):
    'execute subprocess'
    verbose = kw.pop('verbose', 1)
    # expect = kw.pop('expect', None)
    if verbose > 0:
        print(_quote(args), file=sys.stderr)
    proc = Popen(args, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    if verbose > 1:
        getattr(sys.stderr, 'buffer', sys.stderr).write(out)
        getattr(sys.stderr, 'buffer', sys.stderr).write(err)
    if kw.get('stdout') is PIPE:
        proc.stdout.close()
    if kw.get('stderr') is PIPE:
        proc.stderr.close()
    # FIXME: re-write this logic completely
    rc = proc.returncode  # b'error' not in err or proc.returncode
    # if expect is not None and (proc.returncode != expect or rc is True):
    #     print('ERROR: Expected %r, got %r' % (expect, proc.returncode))
    #     if verbose == 1:
    #         getattr(sys.stderr, 'buffer', sys.stderr).write(out)
    #         getattr(sys.stderr, 'buffer', sys.stderr).write(err)
    #     raise SystemExit(1)
    # try to detect error in execution by output
    return rc, out, err


def _remove(path, verbose=1):
    'remove file'
    if os.path.exists(path):
        os.unlink(path)
        if verbose > 0:
            print('del ' + _quote(path))


# TODO: some auto-magic?
def default(options):
    'No command default'
    return 1


# MSSQL LocalDB support
# https://docs.microsoft.com/sql/database-engine/configure-windows/sql-server-express-localdb
# https://docs.microsoft.com/sql/tools/sqlcmd-utility
# TODO: support older isql/osql (unlikely)
# TODO: allow customizing of sqlcmd options
# TODO: support custom collations
# TODO: support running initial scripts
# TODO: check return values
# TODO: filter output via custom logging
# FIXME: sqllocaldb returns exit code 0 regardless of errors. check stderr?
def localdb(options):
    """sqllocaldb wrapper to re-create database instances easily

    uses LocalDB's sqllocaldb utility to create/drop instances
    requries sqlcmd tool to execute queries
    """
    rc = 0
    opts = vars(options)
    verbose = opts['verbose']
    server = '(localdb)\\' + options.instance
    is_sysdb = options.database.lower() in LOCALDB_SYSTEM_DATABASES
    path = os.path.abspath(options.path) if options.path else None
    # enforce trailing sep so it's either empty or full prefix
    if path and not path.endswith(os.sep):
        path += os.sep
    opts['path'] = path

    # generate SQLAlchemy-compatible url
    # TODO: fix url generation for whitespace and unicode
    # TODO: detect driver= option via registry, for example
    # TODO: django database url support. and maybe other orms
    if options.action == 'url':
        url = urlunsplit(
            [
                'mssql',
                '(localdb)\\' + options.instance,
                options.database,
                '',
                '',
            ]
        )
        print(url)
        return 0

    # create/drop actions simple create/drop database only
    # full- versions also re-create the whole instance

    # when creating database drop old one first
    if options.action in ('create', 'full-create', 'drop', 'full-drop'):
        if not is_sysdb:  # can't drop system databases
            query = LOCALDB_DROP_DATABASE.format(**opts)
            _run('sqlcmd', '-S', server, '-Q', query, verbose=verbose)

            # if storing db in specific place, clear old files
            if path:
                _remove(os.path.join(path, options.database + '.mdf'))
                _remove(os.path.join(path, options.database + '.ldf'))
            else:
                # guessing default SQL's DataDirectory and filenames here
                # could get them via SMO or registry but too much work
                _path = os.path.expanduser('~')
                _remove(os.path.join(_path, options.database + '.mdf'))
                _remove(os.path.join(_path, options.database + '_log.ldf'))

    # on full- commands, kill instance completely
    if options.action in ('full-create', 'full-drop'):
        _run(
            'sqllocaldb', 'stop', options.instance, '-i', verbose=verbose,
        )
        _run(
            'sqllocaldb', 'delete', options.instance, verbose=verbose,
        )

    # on full- commands, create new instance
    if options.action in ('full-create', 'only-create'):
        args = ['sqllocaldb', 'create', options.instance, '-s']
        if options.version:
            args.insert(-1, options.version)
        rc, out, err = _run(*args, verbose=verbose, expect=0)

    if options.action in ('create', 'full-create', 'only-create'):
        if not is_sysdb:
            query = (
                LOCALDB_CREATE_DATABASE_ON if path else LOCALDB_CREATE_DATABASE
            ).format(**opts)
            rc, out, err = _run(
                'sqlcmd', '-S', server, '-Q', query, verbose=verbose, expect=0,
            )
    return rc


def iisexpress(options):
    'iisexpress wrapper for running debug server'
    iisexpress_exe = os.path.expandvars(
        r'%ProgramFiles%\IIS Express\iisexpress.exe'
    )
    _run(iisexpress_exe, verbose=2)
    return 0


def make_parser():
    'create parser'
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', '-V', action='version', version=version)
    parser.add_argument(
        '-v', dest='verbose', action='count', help='increase verbosity'
    )
    parser.add_argument(
        '-q', dest='verbose', action='store_const', const=0, help='quiet',
    )
    parser.set_defaults(command=default, verbose=1)

    subparsers = parser.add_subparsers(title='command')

    # localdb
    p = subparsers.add_parser('localdb', help='sqllocaldb wrapper')
    p.add_argument(
        'action',
        choices=[
            'url',
            'create',
            'full-create',
            'only-create',
            'drop',
            'full-drop',
        ],
        help='action to perform',
    )
    p.add_argument(
        dest='database',
        nargs='?',
        help='database name [%(default)s]',
        default='master',
    )
    p.add_argument(
        dest='instance',
        nargs='?',
        help='localdb instance name [%(default)s]',
        default='MSSQLLocalDB',
    )
    p.add_argument(
        '-p', dest='path', help='path to database files [%(default)s]',
    )
    p.add_argument(
        '-v',
        dest='version',
        help='SQL version to use when creating instance [%(default)s]',
    )
    p.set_defaults(command=localdb)

    # iisexpress
    p = subparsers.add_parser('iisexpress', help='iisexpress wrapper')
    p.set_defaults(command=iisexpress)

    return parser


def main(args=None):
    'cli tool for deployment tasks'
    parser = make_parser()
    options = parser.parse_args(args)
    return options.command(options)


if __name__ == '__main__':
    sys.exit(main())
