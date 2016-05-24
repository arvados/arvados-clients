#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""run_bulk_pipelines.py - Start multiple pipeline instances from a template

Usage: run_bulk_pipelines.py template_uuid project_uuid [param [param ...]]

This script starts multiple pipeline instances from a single template, with
different parameters you specify.  It also creates a new subproject to hold
each pipeline instance and its associated data, under the specified project.

Each pipeline instance is started with parameters specified on the command
line, using the same syntax as arv-run-pipeline-instance.  It also reads
parameters from standard input by default, or one or more input files named
with `--input`.  Each line of the input creates a new pipeline instance, with
the parameters on that line.  The input follows shell quoting rules, so if you
need to include a literal newline, quote, or backslash in an input parameter,
you can write that in the input the same way you would write it when you run
arv-run-pipeline-instance in your shell.

If an input line sets a `PROJECT_NAME` parameter (`PROJECT_NAME="..."`), that
will be used as the name for the subproject created for the instance.  You
can change the name of this parameter with the `--project-name-param` option.
This parameter will not be passed to the pipeline instance.
"""
# Copyright 2016 Curoverse
# Written by Brett Smith <brett@curoverse.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import collections
import functools
import io
import locale
import logging
import shlex
import string
import subprocess
import sys
import time

import arvados

from arvados.api import OrderedJsonModel

logger = logging.getLogger('run_bulk_pipelines')

class ParameterParser(shlex.shlex, object):
    # shlex module uses iso-8859-1 encoding, at least in Python 2.7.
    SHLEX_CODING = 'iso-8859-1'

    def __init__(self, instream=None, infile=None):
        self.token = None
        super(ParameterParser, self).__init__(instream, infile, posix=True)
        self.wordchars = self.wordchars.decode(self.SHLEX_CODING)
        addchars = set(unicode(string.punctuation))
        addchars.difference_update(
            (self.quotes + self.escape + self.escapedquotes).decode(self.SHLEX_CODING))
        self.wordchars += ''.join(addchars)

    def read_token(self):
        self.token_lineno = self.lineno
        return super(ParameterParser, self).read_token()

    @property
    def lineno(self):
        return self._lineno

    @lineno.setter
    def lineno(self, value):
        self._lineno = value
        if not self.token:
            self.token_lineno = self._lineno

    def iterlines(self):
        """Iterate logical lines in the source.

        This method yields a list of tokens up to each unescaped newline in
        the source.
        """
        line = []
        on_lineno = self.lineno
        for token in self:
            for _ in xrange(on_lineno, self.token_lineno):
                yield line
                line = []
            line.append(token)
            on_lineno = self.token_lineno + token.count('\n')
        for _ in xrange(on_lineno, self.lineno):
            yield line
            line = []
        if line:
            yield line

    @classmethod
    def iter_parameters(cls, token_seq):
        assign_name = None
        for token in token_seq:
            if assign_name is not None:
                yield (assign_name, token)
                assign_name = None
            else:
                try:
                    name, value = token.split('=', 1)
                except ValueError:
                    assign_name = token
                else:
                    yield (name, value)


class PipelineRun(object):
    def __init__(self, project_uuid, template_uuid, parameters,
                 project_name=None, dry_run=False):
        self.parent_project_uuid = project_uuid
        self.template_uuid = template_uuid
        self.parameters = ['{}={}'.format(key, parameters[key]) for key in parameters]
        if project_name is None:
            keys = list(self.parameters)
            if not keys:
                param_desc = "no parameters specified"
            else:
                keys.sort(key=lambda s: s.rsplit(':', 1)[-1])
                param_desc = " ".join(parameters[key] for key in keys[:3])
            project_name = "Run with " + param_desc
        self.project_name = project_name
        self.dry_run = dry_run
        self.error = None
        self.returncode = None

    def _create_subproject(self, arv, num_retries):
        logger.info("Creating project under %s named %r",
                    self.parent_project_uuid, self.project_name)
        if self.dry_run:
            return 'dry run'
        create_call = arv.groups().create(body={
            'group_class': 'project',
            'owner_uuid': self.parent_project_uuid,
            'name': self.project_name,
        }, ensure_unique_name=True)
        try:
            subproject = create_call.execute(num_retries=num_retries)
        except Exception as error:
            logger.warning("Failed to create project: %s", error,
                           exc_info=logger.isEnabledFor(logging.DEBUG))
            self._save_error(error)
            return None
        else:
            return subproject['uuid']

    def _submit_pipeline(self, project_uuid):
        self.cmd = ['arv', 'pipeline', 'run', '--submit',
                    '--template', self.template_uuid,
                    '--project-uuid', project_uuid] + self.parameters
        if logger.isEnabledFor(logging.DEBUG):
            log_args = ("Running pipeline command %r", self.cmd)
        else:
            log_args = ("Running pipeline with parameters %r", self.parameters)
        logger.info(*log_args)
        if self.dry_run:
            self.returncode = 0
            return None
        proc = subprocess.Popen(self.cmd, stdin=subprocess.PIPE)
        proc.stdin.close()
        return proc

    def submit(self, arv, num_retries):
        project_uuid = self._create_subproject(arv, num_retries)
        if project_uuid is not None:
            self.pipeline_proc = self._submit_pipeline(project_uuid)

    def _wraps_proc(orig_func):
        @functools.wraps(orig_func)
        def proc_wrapper(self):
            if self.pipeline_proc is not None:
                orig_func(self)
            return self.returncode
        return proc_wrapper

    @_wraps_proc
    def poll(self):
        self.returncode = self.pipeline_proc.poll()
        if (self.returncode is not None) and (self.returncode != 0):
            logger.warning("Pipeline run failed with exit code %d", self.returncode)
            self.error = subprocess.CalledProcessError(self.returncode, self.cmd)

    @_wraps_proc
    def wait(self):
        self.pipeline_proc.wait()
        self.poll()

    def success(self):
        if self.error is not None:
            return False
        returncode = self.poll()
        return None if (returncode is None) else (returncode == 0)


class PipelineCoordinator(object):
    def __init__(self, project_uuid, template_uuid, project_name_param,
                 base_parameters, arv_client, dry_run=False):
        self.project_uuid = project_uuid
        self.template_uuid = template_uuid
        self.project_name_param = project_name_param
        self.base_parameters = base_parameters
        self.arv_client = arv_client
        self.dry_run = dry_run
        self.pipelines = []

    def wait_all(self):
        for run in self.pipelines:
            run.wait()

    def losing_streak(self):
        losing_streak = 0
        for run in reversed(self.pipelines):
            success = run.success()
            if success:
                break
            elif success is not None:
                losing_streak += 1
        return losing_streak

    def run_count(self):
        return len(self.pipelines)

    def failures_count(self):
        return sum(1 for run in self.pipelines if run.success() is False)

    def _start_one(self, param_tokens, num_retries):
        parameters = self.base_parameters.copy()
        parameters.update(ParameterParser.iter_parameters(param_tokens))
        project_name = parameters.pop(self.project_name_param, None)
        pipeline = PipelineRun(self.project_uuid, self.template_uuid,
                               parameters, project_name, dry_run=self.dry_run)
        pipeline.submit(self.arv_client, num_retries)
        self.pipelines.append(pipeline)
        return pipeline

    def run(self, params_seq):
        params_iter = iter(params_seq)
        try:
            self._start_one(next(params_iter), 0)
        except StopIteration:
            logger.info("No pipeline run parameters found")
            return True
        self.wait_all()
        if self.failures_count():
            logger.error("Aborting because the first pipeline submission failed")
            return False
        for params in params_iter:
            self._start_one(params, 10)
            time.sleep(0 if self.dry_run else 1)
            if self.losing_streak() >= 3:
                logger.error("Aborting because three consecutive pipeline submissions failed")
                return False
        return True


def parse_arguments(arglist):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-I', default='-', metavar='FILENAME',
                        help="Read instance inputs from this file"
                        " (default stdin)")
    parser.add_argument('--encoding', '-E', default=locale.getpreferredencoding(),
                        help="Input encoding")
    parser.add_argument('--project-name-param', '-P', default='PROJECT_NAME', metavar='PARAMETER',
                        help="Parameter name that sets a pipeline instance's subproject name")
    parser.add_argument('--dry-run', '-n', action='store_true',
                        help="Don't create any Arvados objects, just log what would happen"
                        " (implies -v)")
    parser.add_argument('--verbose', '-v', action='count', default=0,
                        help="Log more information about operation."
                        " Can be specified multiple times for more details.")
    parser.add_argument('template_uuid',
                        help="UUID of the pipeline template to run")
    parser.add_argument('project_uuid',
                        help="UUID of the project to hold subproject runs")
    parser.add_argument('parameters', nargs='*', default=[], metavar='parameter',
                        help="Pipeline instance parameters, in arv-run-pipeline-instance format")
    args = parser.parse_args(arglist)
    if args.dry_run:
        args.verbose = max(1, args.verbose)
    args.log_level = max(1, logging.WARNING - (10 * args.verbose))
    return args

def setup_logger(stream, log_level):
    logger.setLevel(log_level)
    if logger.isEnabledFor(logging.DEBUG):
        log_fmt = '%(asctime)s %(name)s[%(process)d] %(levelname)s: %(message)s'
    else:
        log_fmt = '%(name)s: %(message)s'
    log_handler = logging.StreamHandler(stream)
    log_handler.setFormatter(logging.Formatter(log_fmt, '%Y-%m-%d %H:%M:%S'))
    logger.addHandler(log_handler)

def main(arglist, stdin, stdout, stderr):
    args = parse_arguments(arglist)
    setup_logger(stderr, args.log_level)
    input_source = stdin.fileno() if (args.input == '-') else args.input
    base_parameters = collections.OrderedDict(ParameterParser.iter_parameters(args.parameters))
    try:
        in_file = io.open(input_source, encoding=args.encoding)
    except EnvironmentError as error:
        logger.error("Failed to open %r: %s", args.input, error,
                     exc_info=logger.isEnabledFor(logging.DEBUG))
        return 1
    with in_file:
        try:
            arv = arvados.api('v1')
        except Exception as error:
            logger.error("Failed to connect to Arvados API server: %s", error,
                         exc_info=logger.isEnabledFor(logging.DEBUG))
            return 1
        in_parser = ParameterParser(in_file, args.input)
        coordinator = PipelineCoordinator(args.project_uuid, args.template_uuid,
                                          args.project_name_param,
                                          base_parameters, arv,
                                          dry_run=args.dry_run)
        started_all = coordinator.run(in_parser.iterlines())
    started_count = coordinator.run_count()
    if not started_all:
        logger.error("Aborted after %d pipeline submissions", started_count)
    coordinator.wait_all()
    failures_count = coordinator.failures_count()
    if failures_count:
        logger.error("%d of %d pipeline submissions failed", started_count, failures_count)
        return min(127, 10 + failures_count)
    else:
        logger.info("Submitted %d pipelines", started_count)
        return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:], sys.stdin, sys.stdout, sys.stderr))
