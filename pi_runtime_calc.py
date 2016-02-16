#!/usr/bin/env python2
"""Calculate the compute-hours used by given Arvados pipeline instance(s).

Usage: pi_runtime_calc.py instance_uuid [instance_uuid ...]

This tool will walk the components of a pipeline instance, and generate a CSV
on stdout with information about how much time nodes were allocated for each
component, and the size(s) of those nodes.

NOTE: This tool currently only knows how to determine node sizes for
pipeline instances run on Azure clouds with D-class v2 node sizes.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import collections
import csv
import datetime
import re
import sys

import arvados

from arvados.api import OrderedJsonModel
from ciso8601 import parse_datetime_unaware as parse_ts

class NodeSize(object):
    def __init__(self, name, cores, ram_mb):
        self.name = name
        self.cores = cores
        self.ram_mb = ram_mb

    def can_run(self, job):
        return ((job.constraint('min_cores_per_node', 1) <= self.cores) and
                (job.constraint('min_ram_mb_per_node', 0.0) <= self.ram_mb))


class NodeSizeList(list):
    # You must put sizes in the list cheapest to most expensive.
    def cheapest_size(self, job):
        for size in self:
            if size.can_run(job):
                return size
        raise LookupError("job {} is unsatisfiable".format(job.uuid))


# Azure-specific for now.
SIZE_LIST = NodeSizeList([NodeSize('D1v2', 1, 3.5 * 1024),
                          NodeSize('D2v2', 2, 7 << 10),
                          NodeSize('D11v2', 2, 14 << 10),
                          NodeSize('D3v2', 4, 14 << 10),
                          NodeSize('D12v2', 4, 28 << 10),
                          NodeSize('D4v2', 8, 28 << 10),
                          NodeSize('D13v2', 8, 56 << 10),
                          NodeSize('D5v2', 16, 56 << 10),
                          NodeSize('D14v2', 16, 112 << 10)])

class ArvadosObject(object):
    def __init__(self, object_record):
        self.__dict__.update(object_record)


class Job(ArvadosObject):
    def have_child_jobs(self):
        command = iter(self.script_parameters.get('command', []))
        for word in command:
            if (word == '-jobRunner') and (next(command, '') == 'Arvados'):
                return True
        return False

    def read_log_stats(self):
        self.child_jobs = []
        log_coll = arvados.collection.CollectionReader(self.log)
        log_filename = next(iter(log_coll))
        child_jobs_re = re.compile(' Queued job ({})$'.format(
            arvados.util.uuid_pattern.pattern))
        with log_coll.open(log_filename) as log_file:
            for line in log_file:
                match = child_jobs_re.search(line)
                if match:
                    self.child_jobs.append(match.group(1))

    def constraint(self, constraint_key, base):
        val_type = type(base)
        try:
            setting = val_type(self.runtime_constraints[constraint_key])
        except (KeyError, ValueError):
            return base
        else:
            return max(base, setting)

    def runtime(self):
        node_count = self.constraint('min_nodes', 1)
        start_time = parse_ts(self.started_at)
        end_time = parse_ts(self.finished_at)
        return (end_time - start_time) * node_count

    def node_size(self):
        return SIZE_LIST.cheapest_size(self).name


class PipelineInstance(ArvadosObject):
    def components_runtime(self, arv):
        for cname in self.components:
            runtimes = collections.defaultdict(datetime.timedelta)
            try:
                jobs = [Job(self.components[cname]['job'])]
            except KeyError:
                continue
            while jobs:
                child_job_uuids = []
                for job in jobs:
                    try:
                        runtimes[job.node_size()] += job.runtime()
                    except TypeError:
                        continue  # Job didn't finish.
                    if job.have_child_jobs():
                        try:
                            if not job.log:
                                raise arvados.errors.NotFoundError()
                            job.read_log_stats()
                        except arvados.errors.NotFoundError:
                            pass
                        else:
                            child_job_uuids.extend(job.child_jobs)
                jobs = [Job(job_record) for job_record in arvados.util.list_all(
                    arv.jobs().list, filters=[['uuid', 'in', child_job_uuids]])]
            yield cname, runtimes

    @staticmethod
    def _node_size_key(node_size):
        return (len(node_size), node_size)

    def components_runtime_csv(self, arv, outcsv):
        for cname, runtimes in self.components_runtime(arv):
            for node_size in sorted(runtimes, key=self._node_size_key):
                outcsv.writerow([self.uuid, cname, node_size, runtimes[node_size]])


def parse_arguments(arglist):
    parser = argparse.ArgumentParser()
    parser.add_argument('uuids', metavar='UUID', nargs='+')
    return parser.parse_args(arglist)

def main(stdin, stdout, stderr, arglist, arv):
    args = parse_arguments(arglist)
    pi_records = arvados.util.list_all(arv.pipeline_instances().list,
                                       filters=[['uuid', 'in', args.uuids]])
    outcsv = csv.writer(stdout)
    outcsv.writerow(['Pipeline Instance UUID', 'Component Name',
                     'Node Size', 'Total Runtime'])
    for pi_record in pi_records:
        PipelineInstance(pi_record).components_runtime_csv(arv, outcsv)

if __name__ == '__main__':
    main(sys.stdin, sys.stdout, sys.stderr, sys.argv[1:],
         arvados.api('v1', model=OrderedJsonModel()))
