from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import re

import arvados

class JobLogLine(object):
    @classmethod
    def new_or_append(cls, line):
        try:
            new_line = cls(line)
        except ValueError:
            JobLogLine.last_line.text += line
        else:
            JobLogLine.last_line = new_line
        return JobLogLine.last_line

    def __init__(self, line):
        timestamp, job_uuid, pid, task_num, text = line.split(' ', 4)
        self.timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%d_%H:%M:%S')
        if arvados.util.uuid_pattern.match(job_uuid) is None:
            raise ValueError("{} not a UUID".format(job_uuid))
        self.job_uuid = job_uuid
        self.pid = int(pid)
        self.task_num = int(task_num) if task_num else None
        self.text = text


class UUIDMapper(object):
    def __init__(self, arv):
        self._prefix_to_class_names = {
            schema['uuidPrefix']: self._api_to_class_name(api_name, arv)
            for api_name, schema in arv._schema.schemas.iteritems()
            if schema.get('uuidPrefix')}

    def _api_to_class_name(self, api_name, arv):
        class_name = re.sub(r'(.)([A-Z])', r'\1_\2', api_name).lower()
        underscore_count = class_name.count('_')
        arv_class_match = ''
        for arv_class in dir(arv):
            if (arv_class.startswith(class_name) and
                  (arv_class.count('_') == underscore_count) and
                  (len(arv_class) > len(arv_class_match))):
                arv_class_match = arv_class
        return arv_class_match

    def class_name(self, uuid):
        if arvados.util.portable_data_hash_pattern.match(uuid):
            return 'collections'
        uuid_type = uuid.split('-', 2)[1]
        return self._prefix_to_class_names[uuid_type]

    def arv_class(self, uuid, arv):
        return getattr(arv, self.class_name(uuid))()
