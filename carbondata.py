#!/usr/bin/env python

import os
import inspect
import argparse
import json
import subprocess


class Env(object):

    virtual_env = ".env"

    requirements = [
        'docopt',
        'thriftpy'
    ]

    def __init__(self, args, virtual=False):
        self._carbondata_root = args.path or os.environ.get('CARBONDATA_PATH', os.getcwd())
        self._modules = {}
        self._use_virtual = virtual
        self._session()

    def _session(self):
        if self._use_virtual:
            if os.path.exists(self.path(self.virtual_env)):
                execfile(self.path(self.virtual_env, 'bin', 'activate_this.py'), {}, {
                    '__file__': self.path(self.virtual_env, 'bin', 'activate_this.py')
                })

    def _load(self, name, alias=None):
        if name.count('.') == 0:
            alias = alias or name
            module = self._modules[alias] = __import__(name, None, None)
            return
        parts = name.split('.')
        alias = alias or parts[-1]
        obj = __import__('.'.join(parts[:-1]), None, None, [parts[-1]], 0)
        try:
            module = self._modules[alias] = getattr(obj, parts[-1])
            return module
        except AttributeError:
            raise ImportError('No module named {0}'.format(parts[-1]))

    def _load_modules(self):
        if not self._modules:
            self._load('docopt.docopt')
            self._load('thriftpy')
            self._load('thriftpy.protocol.binary.TBinaryProtocol')
            self._load('thriftpy.protocol.json.struct_to_json')

    def module(self, name):
        self._load_modules()
        try:
            return self._modules[name]
        except KeyError:
            raise ImportError('No module named {0}'.format(name))

    def __getitem__(self, name):
        return self.module(name)

    def install(self):
        if self._use_virtual:
            if not os.path.exists(self.path(self.virtual_env)):
                subprocess.call([
                    'virtualenv',
                    self.virtual_env,
                ], cwd=self.path())
        self._session()
        subprocess.call([
            'pip',
            'install'
        ] + self.requirements)

    def path(self, *args):
        return os.path.join(self._carbondata_root, *args)

    def get_thrift_path(self, name):
        return self.path('format', 'src', 'main', 'thrift', '{0}.thrift'.format(name))


class DisplayFile(object):

    def __init__(self, env, file_path):
        self._env = env
        self._file = file_path

    def display(self):
        raise NotImplementedError()


class ThriftFile(DisplayFile):

    thrift_struct = '<name>.<class>'

    def display(self):
        group, cls = self.thrift_struct.split('.')
        thrift = self._env['thriftpy'].load(self._env.get_thrift_path(group))
        data = []
        with open(self._file, 'rb') as fp:
            while True:
                i = fp.tell()
                try:
                    struct = getattr(thrift, cls)()
                    struct.read(self._env['TBinaryProtocol'](fp))
                    data.append(self._env['struct_to_json'](struct))
                    if fp.tell() == i:
                        break
                except Exception:
                    break
        return data


class SchemaFile(ThriftFile):

    thrift_struct = 'schema.TableInfo'


class DictFile(ThriftFile):

    thrift_struct = 'dictionary.ColumnDictionaryChunk'


class DictMetaFile(ThriftFile):

    thrift_struct = 'dictionary_meta.ColumnDictionaryChunkMeta'


class SortIndexFile(ThriftFile):

    thrift_struct = 'sort_index.ColumnSortInfo'


class CarbonIndexFile(DisplayFile):

    def display(self):
        thrift = self._env['thriftpy'].load(self._env.get_thrift_path('carbondataindex'))
        data = []
        with open(self._file, 'rb') as fp:
            while True:
                i = fp.tell()
                try:
                    struct = getattr(thrift, 'IndexHeader')()
                    struct.read(self._env['TBinaryProtocol'](fp))
                    data.append(self._env['struct_to_json'](struct))
                    if fp.tell() == i:
                        break
                except Exception:
                    break
        return data


class TableStatusFile(DisplayFile):

    def display(self):
        with open(self._file, 'rb') as fp:
            return json.loads(fp.read())


class Command(object):

    def display(self, env, args):
        """Display carbondata

        Usage:
            carbondata.py display FILE

        Options:
            -h --help   Display help

        """
        dfile = None
        if args['FILE'].endswith('schema'):
            dfile = SchemaFile(env, args['FILE'])
        elif args['FILE'].endswith('tablestatus'):
            dfile = TableStatusFile(env, args['FILE'])
        elif args['FILE'].endswith('.dict'):
            dfile = DictFile(env, args['FILE'])
        elif args['FILE'].endswith('.dictmeta'):
            dfile = DictMetaFile(env, args['FILE'])
        elif args['FILE'].endswith('.sortindex'):
            dfile = SortIndexFile(env, args['FILE'])
        elif args['FILE'].endswith('.carbonindex'):
            dfile = CarbonIndexFile(env, args['FILE'])

        if dfile:
            print(json.dumps(dfile.display(), indent=2))
        else:
            print('Illegal file {0}.'.format(args['FILE']))

    def build(self, env, args):
        """Build carbondata project

        Usage:
            carbondata.py build

        Options:
            -h --help   Display help
        """
        subprocess.call('mvn -DskipTests clean install',
                shell=True, cwd=env.path())

    def example(self, env, args):
        """Run carbondata example

        Usage:
            carbondata.py example CLASS [--build] [--debug]

        Options:
            -h --help   Display help
            --build     rebuild
            --debug     debug mode
            CLASS       class in org.carbondata.examples
        """
        if args['--build']:
            self.build(env, {})
        executor = 'mvn'
        if args['--debug']:
            executor = 'mvnDebug'
        subprocess.call('{0} exec:java -Dexec.mainClass="org.carbondata.examples.{1}"'.format(executor, args['CLASS']),
            shell=True, cwd=env.path('examples'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Carbondata script')
    parser.add_argument('--path', help='carbondata root')
    parser.add_argument('--virtual', action='store_true', default=False, help='use virutal env')
    sub_parser = parser.add_subparsers(help='carbon commands')
    sub_parser.add_parser('install', help='install dependencies').set_defaults(install=True)
    command = Command()
    for cmd in Command.__dict__:
        if not cmd.startswith('_') and inspect.ismethod(getattr(command, cmd)):
            display_parser = sub_parser.add_parser(cmd, add_help=False)
            display_parser.set_defaults(func=getattr(command, cmd))
    args = parser.parse_known_args()
    env = Env(args[0], args[0].virtual)
    if getattr(args[0], 'install', False):
        env.install()
    else:
        cmd_args = env['docopt'](args[0].func.__doc__, argv=[args[0].func.__name__] + args[1])
        cmd_args.pop(args[0].func.__name__, None)
        args[0].func(env, cmd_args)

