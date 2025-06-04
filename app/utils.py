import time
import json
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
    try:
        return int(value)
    except ValueError:
        return None
    
    
def remove_trailing_commas(json_string: str) -> str:
    # # Remove trailing commas before closing braces or brackets
    # json_string = re.sub(r',(\s*[}\]])', r'\1', json_string)
    # # Remove trailing commas before newlines or closing braces in objects/arrays
    # json_string = re.sub(r',\s*([}\]])', r'\1', json_string)
    # return json_string
    
    find_trailing_comma = r',(?!\s*[\{\[\"\']\w)'
    pattern = re.compile(find_trailing_comma)
    json_string = pattern.sub('', json_string)
    return json_string
    

    # try:
    #     # Step 1: Normalize the input string
    #     json_string = json_string.strip().strip('"\'')  # Remove leading/trailing quotes
        
    #     # Step 2: Replace Python-specific values
    #     json_string = (json_string.replace("None", "")
    #                   .replace("True", "true")
    #                   .replace("False", "false")
    #                   .replace("[None, None]", "[]"))
        
    #     # Step 3: Escape unescaped double quotes within string values
    #     def escape_double_quotes(match):
    #         content = match.group(1)
    #         # Replace unescaped double quotes with escaped ones, but skip already escaped quotes
    #         content = re.sub(r'(?<!\\)"', r'\"', content)
    #         return f'"{content}"'
        
    #     # Apply to double-quoted strings (e.g., "Fonctionnaire de l"État" → "Fonctionnaire de l\"État")
    #     json_string = re.sub(r'"([^"]*?)"', escape_double_quotes, json_string)
        
    #     # Step 4: Convert single quotes to double quotes for keys and string boundaries
    #     # Replace single quotes around keys (e.g., 'key': → "key":)
    #     json_string = re.sub(r"(\w+)'(?=\s*:)", r'\1"', json_string)
    #     json_string = re.sub(r"'\s*(:)", r'"\1', json_string)
        
    #     # Replace single quotes at string boundaries, preserving content
    #     def preserve_content(match):
    #         content = match.group(1)
    #         return f'"{content}"'  # Wrap in double quotes, preserve internal single quotes
        
    #     # Apply to single-quoted strings (e.g., 'Fonctionnaire de l'État' → "Fonctionnaire de l'État")
    #     json_string = re.sub(r"'([^']*?)'", preserve_content, json_string)
        
    #     # Step 5: Fix common escaping issues (after preserving content)
    #     json_string = json_string.replace('\\"', '"').replace('\\\'', '\'')
        
    #     # Step 6: Remove trailing commas
    #     find_trailing_comma = r',(?!\s*[\{\[\"\']\w|\s*\d)'
    #     json_string = re.sub(find_trailing_comma, '', json_string)
    #     json_string = re.sub(r'\s*,\s*([}\]])', r'\1', json_string)
        
    #     # Step 7: Fix missing commas between objects/arrays
    #     json_string = re.sub(r'}\s*{', r'}, {', json_string)
    #     json_string = re.sub(r']\s*\[', r'], [', json_string)
    #     json_string = re.sub(r'}\s*\[', r'}, [', json_string)
    #     json_string = re.sub(r']\s*{', r'], {', json_string)
        
    #     # Step 8: Remove duplicate commas
    #     json_string = re.sub(r',+', ',', json_string)
        
    #     # Step 9: Normalize whitespace
    #     json_string = re.sub(r'\s+', ' ', json_string).strip()
        
    #     return json_string
    # except Exception as e:
    #     logger.error(f"Error cleaning JSON string: {str(e)}")
    #     return json_string  # Return original string if cleaning fails