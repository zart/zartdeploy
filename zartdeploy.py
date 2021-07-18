'Deploy tool'
import sys
import argparse

version = '0.1.0'


def default(options):
    'No command default'
    return 1


def make_parser():
    'create parser'
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', '-V', action='version', version=version)
    parser.set_defaults(command=default)
    parser.add_subparsers(title='command')

    return parser


def main(args=None):
    'cli tool for deployment tasks'
    parser = make_parser()
    options = parser.parse_args(args)
    return options.command(options)


if __name__ == '__main__':
    sys.exit(main())
