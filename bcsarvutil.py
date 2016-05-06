import re

import arvados

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
