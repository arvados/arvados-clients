#!/usr/bin/env python2
"""Calculate the compute-hours used by given Arvados pipeline instance(s)."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import datetime
import sys

import arvados

from ciso8601 import parse_datetime_unaware as parse_ts

def parse_arguments(arglist):
    parser = argparse.ArgumentParser()
    parser.add_argument('uuids', metavar='UUID', nargs='+')
    return parser.parse_args(arglist)

def job_runtime(job):
    try:
        node_count = max(1, int(job['runtime_constraints']['min_nodes']))
    except (KeyError, ValueError):
        node_count = 1
    start_time = parse_ts(job['started_at'])
    end_time = parse_ts(job['finished_at'])
    return (end_time - start_time) * node_count

def pi_runtime(pipeline_instance):
    return sum((job_runtime(component['job'])
                for component in pipeline_instance['components'].itervalues()),
               datetime.timedelta())

def main(stdin, stdout, stderr, arglist, arv):
    args = parse_arguments(arglist)
    p_instances = arv.pipeline_instances().list(
        filters=[['state', '=', 'Complete'],
                 ['uuid', 'in', args.uuids]]).execute()
    for pipeline_instance in p_instances['items']:
        print('{:30} {}'.format(pipeline_instance['uuid'],
                                pi_runtime(pipeline_instance)), file=stdout)

if __name__ == '__main__':
    main(sys.stdin, sys.stdout, sys.stderr, sys.argv[1:], arvados.api('v1'))
