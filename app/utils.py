import time
import json
import ast
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None
    
    
def remove_trailing_commas(py_dict_str: str) -> str:
    # Fix missing commas between adjacent objects in arrays
    py_dict_str = re.sub(r'\}\s*\{', '},{', py_dict_str)
    
    # Remove trailing commas in arrays/objects
    py_dict_str = re.sub(r',\s*\]', ']', py_dict_str)  # Arrays
    py_dict_str = re.sub(r',\s*\}', '}', py_dict_str)   # Objects
    
    # Parse as Python dict and convert to valid JSON
    parsed_dict = ast.literal_eval(py_dict_str)
    return json.dumps(parsed_dict, ensure_ascii=False)
