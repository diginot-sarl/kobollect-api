import time
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


def generate_nif():
    # Get the current timestamp
    timestamp = int(time.time())
    
    # Convert the timestamp to base 36
    base36_timestamp = base36_encode(timestamp)
    
    # Create the final code
    code = f"NTD2-{base36_timestamp}"
    return code

def base36_encode(number):
    """Convert an integer to a base36 string (uppercase)."""
    alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    if number < 0:
        raise ValueError("Number must be a non-negative integer")
    base36 = ''
    while number > 0:
        number, i = divmod(number, 36)
        base36 = alphabet[i] + base36
    return base36 or '0'

def safe_int(value):
    try:
        return int(value)
    except ValueError:
        return None