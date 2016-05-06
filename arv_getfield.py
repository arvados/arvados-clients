#!/usr/bin/env python
"""arv_getfield.py - Show and save Arvados object fields

Usage: arv_getfield.py uuid.field[.field...] [destination]
Example: arv_getfield.py zzzzz-d1hrv.components.cname.job.log logdir/

If the referenced field is a collection, it will be downloaded to the
destination, by default the current directory.
Otherwise, the field's JSON will be written to the destination, by default
stdout.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import json
import os
import subprocess
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'bcsarvutil.py'))

import arvados

from bcsarvutil import UUIDMapper

def walk_fields(uuid_with_fields, arv, uuid_mapper):
    fields = iter(uuid_with_fields.split('.'))
    uuid = next(fields)
    arv_class = uuid_mapper.arv_class(uuid, arv)
    result = arv_class.get(uuid=uuid).execute()
    for field_name in fields:
        result = result[field_name]
    return result

def walk_files(collection):
    search_queue = [collection]
    search_index = 0
    while search_index < len(search_queue):
        search_coll = search_queue[search_index]
        search_index += 1
        for key in search_coll:
            item = search_coll[key]
            if isinstance(item, arvados.collection.Collection):
                search_queue.append(item)
            else:
                yield item

def coll_get_target(collection, destination):
    files_iter = walk_files(collection)
    try:
        target_file = next(files_iter)
    except StopIteration:
        raise ValueError("collection is empty")
    if next(files_iter, None) is None:
        stream_name = target_file.parent.stream_name()
        if (stream_name == '.') or stream_name.startswith('./'):
            stream_name = stream_name[2:]
        if stream_name:
            return '/{}/{}'.format(stream_name, target_file.name)
        else:
            return '/{}'.format(target_file.name)
    elif (destination == '-') or (not os.path.isdir(destination)):
        raise ValueError("collection has multiple files; destination must be a directory")
    else:
        return '/'

def parse_arguments(arglist):
    parser = argparse.ArgumentParser()
    parser.add_argument('uuid_with_fields')
    parser.add_argument('destination', nargs='?')
    return parser.parse_args(arglist)

def abort(errmsg, exit_code=1):
    print("ERROR: " + errmsg)
    sys.exit(exit_code)

def show(value, destination, stdout):
    if (destination is None) or (destination == '-'):
        outfile = stdout
    else:
        outfile = open(destination, 'w')
    with outfile:
        json.dump(value, outfile, indent=2)
        outfile.write("\n")
    sys.exit(0)

def main(arglist, stdin, stdout, stderr):
    args = parse_arguments(arglist)
    arv = arvados.api('v1')
    uuid_mapper = UUIDMapper(arv)
    try:
        get_value = walk_fields(args.uuid_with_fields, arv, uuid_mapper)
    except (KeyError, TypeError) as error:
        abort("Failed to find {}: {}".format(args.uuid_with_fields, error))
    if get_value is None:
        show(get_value, args.destination, stdout)
    try:
        collection = arvados.collection.CollectionReader(get_value)
    except (TypeError, arvados.errors.ArgumentError) as error:
        show(get_value, args.destination, stdout)
    destination = '.' if (args.destination is None) else args.destination
    try:
        target = coll_get_target(collection, destination)
    except ValueError as error:
        abort("Can't download {}: {}".format(get_value, error))
    get_status = subprocess.call(['arv-get', get_value + target, destination])
    if get_status != 0:
        sys.exit(get_status)
    elif os.path.isdir(destination) and (target != '/'):
        print("{} downloaded to {}".format(target.lstrip('/'), destination),
              file=stdout)

if __name__ == '__main__':
    main(sys.argv[1:], sys.stdin, sys.stdout, sys.stderr)
