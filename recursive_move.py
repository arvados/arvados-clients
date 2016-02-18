#!/usr/bin/env python2
"""recursive_move.py: Recursively move an Arvados object to another project.

Usage: recursive_move.py object_uuid [object_uuid ...] dest_project

This will move an object, and all of its dependencies and metadata, to
dest_project.  This is most helpful to prepare a clean project from a
development project: after you finish work on a pipeline, you can move one
template and/or instance to a sharable project, while leaving all the
development work (other pipeline instances, their jobs and logs, etc.) in
the original project.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import itertools
import logging
import re
import sys

import arvados

from arvados.api import OrderedJsonModel

logger = logging.getLogger('arvados move')

def arv_walk(arv_obj, visit_func, *args, **kwargs):
    if isinstance(arv_obj, dict):
        for key in arv_obj:
            arv_walk(arv_obj[key], visit_func, *args, **kwargs)
    elif isinstance(arv_obj, list):
        for item in arv_obj:
            arv_walk(item, visit_func, *args, **kwargs)
    else:
        visit_func(arv_obj, *args, **kwargs)

def uuid_class(uuid, arv):
    if arvados.util.portable_data_hash_pattern.match(uuid):
        return arv.collections()
    _, uuid_type, _ = uuid.split('-')
    class_name = next(key for key, schema in arv._schema.schemas.iteritems()
                      if schema.get('uuidPrefix') == uuid_type)
    class_name = re.sub(r'(.)([A-Z])', r'\1_\2', class_name).lower()
    arv_class_match = ''
    for arv_class in dir(arv):
        if (arv_class.startswith(class_name) and
              (len(arv_class) > len(arv_class_match))):
            arv_class_match = arv_class
    return getattr(arv, arv_class_match)()

class DependencyTracker(object):
    def __init__(self, logger):
        self.logger = logger
        self.uuids = set()
        # Map UUIDs to objects
        self.copy_uuids = {}

    def _copyable_field(self, key):
        return not key.endswith(('uuid', '_at'))

    def _clean_copy(self, arv_obj):
        return type(arv_obj)((key, arv_obj[key]) for key in arv_obj
                             if self._copyable_field(key))

    def _coll_sort_key(self, src_owner_uuid):
        def coll_key(coll_obj):
            return (1 if coll_obj['owner_uuid'] == src_owner_uuid else 0,
                    1 if coll_obj['name'] else 0,
                    coll_obj['created_at'])
        return coll_key

    def add_object(self, uuid, arv):
        arv_class = uuid_class(uuid, arv)
        arv_obj = self._send_request(arv_class.get(uuid=uuid))
        uuids = set()
        pdhs = set()
        arv_walk(arv_obj, self.add_dependency, uuids, pdhs)
        self.translate_pdhs_to_uuids(arv, pdhs, uuids, arv_obj['owner_uuid'])
        self.add_dependency_links(arv, arv_obj['owner_uuid'], uuids)
        self.uuids.update(uuids)

    def add_dependency(self, item, uuids, pdhs):
        try:
            pdh_match = arvados.util.portable_data_hash_pattern.match(item)
        except TypeError:
            return
        if pdh_match:
            self.logger.debug("found PDH %s", pdh_match.group(0))
            pdhs.add(pdh_match.group(0))
            return
        match = arvados.util.uuid_pattern.match(item)
        if (match and
              not arvados.util.user_uuid_pattern.match(item) and
              not arvados.util.group_uuid_pattern.match(item)):
            self.logger.debug("found UUID %s", match.group(0))
            uuids.add(match.group(0))

    def translate_pdhs_to_uuids(self, arv, pdhs, uuids, owner_uuid):
        all_uuids = self.uuids.union(self.copy_uuids).union(uuids)
        for pdh in pdhs:
            coll_list = arvados.util.list_all(
                arv.collections().list,
                filters=[['portable_data_hash', '=', pdh]],
                select=['uuid', 'owner_uuid', 'name', 'created_at'])
            coll_uuids = [c['uuid'] for c in coll_list]
            self.add_dependency_links(arv, None, coll_uuids)
            if all_uuids.intersection(coll_uuids):
                self.logger.debug("skipping PDH %s already being moved", pdh)
                continue
            coll_list.sort(key=self._coll_sort_key(owner_uuid))
            src_uuid = coll_list[-1]['uuid']
            src_coll = self._send_request(arv.collections().get(uuid=src_uuid))
            self.logger.info("found %s from %s", pdh, src_uuid)
            self.copy_uuids[src_uuid] = src_coll
            all_uuids.add(src_uuid)

    def add_dependency_links(self, arv, owner_uuid, uuids):
        for link_filter in itertools.product(['head_uuid', 'tail_uuid'],
                                             ['in'],
                                             [list(uuids)]):
            self.logger.debug("searching for links matching %r", link_filter)
            for link in arvados.util.list_all(
                    arv.links().list,
                    filters=[list(link_filter)]):
                self.logger.debug("found link %s", link['uuid'])
                self.copy_uuids[link['uuid']] = link

    def _send_request(self, arv_request):
        return arv_request.execute()

    def move_to(self, owner_uuid, arv, request_handler=None):
        if request_handler is None:
            self._handle_request = self._send_request
        else:
            self._handle_request = request_handler
        self._move_uuids_to(owner_uuid, arv)
        self._copy_uuids_to(owner_uuid, arv)

    def _move_uuids_to(self, owner_uuid, arv):
        for uuid in self.uuids:
            obj_class = uuid_class(uuid, arv)
            self.logger.info("moving %s to %s", uuid, owner_uuid)
            self._handle_request(obj_class.update(
                uuid=uuid, body={'owner_uuid': owner_uuid}))

    def _copy_object(self, src_obj, owner_uuid, arv):
        new_obj = self._clean_copy(src_obj)
        new_obj.update(owner_uuid=owner_uuid)
        arv_class = uuid_class(src_obj['uuid'], arv)
        self._handle_request(arv_class.create(body=new_obj))

    def _copy_uuids_to(self, owner_uuid, arv):
        for uuid in self.copy_uuids:
            self.logger.info("copying %s to %s", uuid, owner_uuid)
            self._copy_object(self.copy_uuids[uuid], owner_uuid, arv)


def _noop_request(request):
    pass

def parse_arguments(arglist):
    parser = argparse.ArgumentParser()
    parser.add_argument('--destination', '-t', metavar='UUID',
                        help="Project UUID to move objects to")
    parser.add_argument('--dry-run', '-n', dest='request_handler',
                        action='store_const', const=_noop_request,
                        help="Do not write any changes to Arvados")
    parser.add_argument('--verbose', '-v', action='count', default=0,
                        help="Log more information")
    parser.add_argument('uuids', metavar='UUID', nargs='+')
    args = parser.parse_args(arglist)
    if not args.destination:
        if len(args.uuids) < 2:
            parser.error("missing destination project UUID argument")
        args.destination = args.uuids.pop()
    return args

def setup_logging(args):
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(name)s[%(process)d] %(levelname)s: %(message)s',
            '%Y-%m-%d %H:%M:%S'))
    logger.addHandler(log_handler)
    logger.setLevel(max(1, logging.WARNING - (10 * args.verbose)))

def main(stdin, stdout, stderr, arglist, arv):
    args = parse_arguments(arglist)
    setup_logging(args)
    dependencies = DependencyTracker(logger)
    for uuid in args.uuids:
        dependencies.add_object(uuid, arv)
    dependencies.move_to(args.destination, arv, args.request_handler)

if __name__ == '__main__':
    main(sys.stdin, sys.stdout, sys.stderr, sys.argv[1:],
         arvados.api('v1', model=OrderedJsonModel()))
