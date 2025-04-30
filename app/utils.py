import json

def parse_str_to_json(json_str):
    try:
        json_data = json.loads(json_str)
        return json_data
    except json.JSONDecodeError as e:
        return None
    
def find_object_by_id(data, id):
    for obj in data:
        if obj["id"] == id:
            return obj
    return None