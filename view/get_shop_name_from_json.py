import json

def get_values(json_obj, key, values=[]):
    if isinstance(json_obj, dict):
        for k, v in json_obj.items():
            if k == key:
                values.append(v)
            elif isinstance(v, (dict, list)):
                get_values(v, key, values)
    elif isinstance(json_obj, list):
        for item in json_obj:
            get_values(item, key, values)
    return values