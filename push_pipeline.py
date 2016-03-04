#!/usr/bin/env python
"""push_pipeline.py: Update a pipeline template and Git repository together.

Usage:
  push_pipeline.py dest [refspec [refspec ...]]

An Arvados configuration file must exist at
~/.config/arvados/<dest>.conf.

Configuration is saved in `.git/arvados_pipeline.ini` in your checkout.
You can share this with collaborators to make it easy for them to use
push_pipeline.py on the same pipeline.  They just need to save the file
to the same location in their own checkout before they use push_pipeline.py.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import collections
import ConfigParser
import json
import os
import signal
import subprocess
import sys
import tempfile

import arvados

from arvados.api import OrderedJsonModel, api_from_config

def excepthook(exc_type, exc_value, exc_tb):
    if issubclass(exc_type, subprocess.CalledProcessError):
        sys.exit(exc_value.returncode)
    elif issubclass(exc_type, KeyboardInterrupt):
        sys.exit(128 + signal.SIGINT)
    else:
        sys.__excepthook__(exc_type, exc_value, exc_tb)

class PipelineConfig(ConfigParser.SafeConfigParser):
    def __init__(self, path):
        ConfigParser.SafeConfigParser.__init__(self)
        self._arv_path = path
        self.read(path)

    def save(self):
        with tempfile.NamedTemporaryFile(dir=os.path.dirname(self._arv_path)) as outfile:
            self.write(outfile)
            outfile.flush()
            os.rename(outfile.name, self._arv_path)
            outfile.delete = False

    def get_or_ask(self, section, option, ask_func, *args, **kwargs):
        try:
            return self.get(section, option)
        except ConfigParser.NoOptionError:
            value = ask_func(*args, **kwargs)
            self.set(section, option, value)
            self.save()
            return value


class Prompter(object):
    def __init__(self, source, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr
        try:
            interactive = source.isatty() and stdout.isatty()
        except AttributeError:
            interactive = False
        if interactive:
            self.source = source
            self.ask = self._source_ask
        else:
            self.ask = self._fail_ask

    def _fail_ask(self, prompt, errmsg):
        print("Error: {}.".format(errmsg), file=self.stderr)
        sys.exit(1)

    def _source_ask(self, prompt, errmsg):
        print(prompt, end=': ', file=self.stdout)
        in_line = self.source.readline()
        if not in_line:
            self._fail_ask(self, prompt, errmsg)
        return in_line.rstrip('\n')


class Cluster(object):
    def __init__(self, name, config=()):
        self.name = name
        self.src_config = dict(config)
        self.template_uuid = self.src_config.get('template_uuid')
        self.git_remote = self.src_config.get('git_remote', self.name)
        self.arv_conf_path = self.src_config.get(
            'config_path',
            os.path.expanduser('~/.config/arvados/{}.conf'.format(self.name)))

    def git_urls_cmd(self):
        return ['git', 'config', '--null', '--get-regexp',
                r'^remote\.{}\.[^.]*url$'.format(self.git_remote)]

    def load_git_push_url(self, git_output):
        git_urls = {}
        for line in git_output.split('\0'):
            try:
                full_key, value = line.split('\n', 1)
            except ValueError:
                continue
            git_urls[full_key.split('.', 2)[2]] = value
        if 'push_url' in git_urls:
            self.git_push_url = git_urls['push_url']
        elif 'url' in git_urls:
            self.git_push_url = git_urls['url']
        else:
            self.git_push_url = self.src_config.get('git_push_url')

    def config_changes(self):
        for key in ['template_uuid', 'git_push_url']:
            current_value = getattr(self, key)
            if current_value != self.src_config.get(key):
                yield key, current_value


def parse_arguments(arglist):
    parser = argparse.ArgumentParser()
    parser.add_argument('--noninteractive', '-n',
                        action='store_false', default=True, dest='interactive',
                        help="Never prompt for missing configuration")
    parser.add_argument('dest')
    parser.add_argument('refspecs', metavar='refspec', nargs='*', default=[])
    return parser.parse_args(arglist)

def get_git_dir(rev_switch):
    return subprocess.check_output(['git', 'rev-parse', rev_switch]).rstrip('\n')

def load_pipeline_config(conf_dir, cluster_id):
    conf_path = os.path.join(conf_dir, 'arvados_pipeline.ini')
    pipeline_config = PipelineConfig(conf_path)
    if not pipeline_config.has_section(cluster_id):
        pipeline_config.add_section(cluster_id)
    return pipeline_config

def ask_template_path(prompter, stdout):
    git_work_dir = os.path.realpath(get_git_dir('--show-toplevel'))
    while True:
        raw_path = prompter.ask(
            "Pipeline template file in this checkout",
            "No pipeline template known in this repository")
        abs_path = os.path.realpath(raw_path)
        if os.path.isfile(abs_path) and abs_path.startswith(git_work_dir):
            return os.path.relpath(abs_path, git_work_dir)
        else:
            print("{} not found in checkout".format(raw_path), file=stdout)

def setup_git(cluster, prompter):
    git_output = subprocess.check_output(cluster.git_urls_cmd())
    cluster.load_git_push_url(git_output)
    if cluster.git_push_url is None:
        cluster.git_push_url = prompter.ask(
            "Git push URL for " + cluster.git_remote,
            "No Git push URL known for " + cluster.git_remote)

def main(stdin, stdout, stderr, arglist):
    args = parse_arguments(arglist)
    prompter = Prompter(stdin if args.interactive else None, stdout, stderr)
    git_conf_dir = get_git_dir('--git-dir')
    pipeline_config = load_pipeline_config(git_conf_dir, args.dest)
    cluster = Cluster(args.dest, pipeline_config.items(args.dest))
    if not os.path.exists(cluster.arv_conf_path):
        print("Error: No Arvados configuration at {}.".format(cluster.arv_conf_path),
              file=stderr)
        sys.exit(1)
    arvados.config.initialize(cluster.arv_conf_path)

    template_path = pipeline_config.get_or_ask(
        'DEFAULT', 'template_path', ask_template_path, prompter, stdout)
    if cluster.template_uuid is None:
        cluster.template_uuid = prompter.ask(
            "Pipeline template UUID on " + args.dest,
            "No pipeline template UUID known on " + args.dest)
    setup_git(cluster, prompter)

    with open(template_path) as pt_file:
        pipeline_template = json.load(pt_file, object_pairs_hook=collections.OrderedDict)
    subprocess.check_call(['git', 'push', cluster.git_push_url] + args.refspecs)
    arv = api_from_config('v1', model=OrderedJsonModel())
    arv.pipeline_templates().update(uuid=cluster.template_uuid,
                                    body=pipeline_template).execute()

    config_dirty = False
    for key, value in cluster.config_changes():
        pipeline_config.set(args.dest, key, value)
        config_dirty = True
    if config_dirty:
        pipeline_config.save()

if __name__ == '__main__':
    sys.excepthook = excepthook
    main(sys.stdin, sys.stdout, sys.stderr, sys.argv[1:])
