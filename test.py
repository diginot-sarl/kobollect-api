import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def remove_trailing_commas(json_string: str):
    # # Remove trailing commas before closing braces or brackets
    # json_string = re.sub(r',(\s*[}\]])', r'\1', json_string)
    # # Remove trailing commas before newlines or closing braces in objects/arrays
    # json_string = re.sub(r',\s*([}\]])', r'\1', json_string)
    # return json_string
    

    try:
        # Step 1: Normalize the input string
        json_string = json_string.strip().strip('"\'')  # Remove leading/trailing quotes
        
        # Step 2: Fix common escaping issues
        json_string = json_string.replace('\\"', '"').replace('\\\'', '\'')
        
        # Step 3: Replace Python-specific values
        json_string = (json_string.replace("None", "")
                      .replace("True", "true")
                      .replace("False", "false")
                      .replace("[None, None]", "[]"))
        
        # Step 4: Escape single quotes in string values while converting single quotes to double quotes
        # Match strings within single quotes and escape internal single quotes
        def escape_single_quotes(match):
            content = match.group(1)
            # Escape single quotes within the string
            content = content.replace("'", "\\'")
            return f'"{content}"'
        
        # Replace single-quoted strings with double-quoted strings, escaping internal single quotes
        json_string = re.sub(r"'([^']*)'", escape_single_quotes, json_string)
        
        # Step 5: Replace remaining single quotes with double quotes for keys
        # This handles cases where keys are single-quoted (e.g., 'key': value → "key": value)
        json_string = re.sub(r"(\w+)'(?=\s*:)", r'\1"', json_string)
        json_string = re.sub(r"'\s*(:)", r'"\1', json_string)
        
        # Step 6: Remove trailing commas
        find_trailing_comma = r',(?!\s*[\{\[\"\']\w|\s*\d)'
        json_string = re.sub(find_trailing_comma, '', json_string)
        json_string = re.sub(r'\s*,\s*([}\]])', r'\1', json_string)
        
        # Step 7: Fix missing commas between objects/arrays
        json_string = re.sub(r'}\s*{', r'}, {', json_string)
        json_string = re.sub(r']\s*\[', r'], [', json_string)
        json_string = re.sub(r'}\s*\[', r'}, [', json_string)
        json_string = re.sub(r']\s*{', r'], {', json_string)
        
        # Step 8: Remove duplicate commas
        json_string = re.sub(r',+', ',', json_string)
        
        # Step 9: Normalize whitespace
        json_string = re.sub(r'\s+', ' ', json_string).strip()
        
        return json_string
    except Exception as e:
        logger.error(f"Error cleaning JSON string: {str(e)}")
        return json_string  # Return original string if cleaning fails
    
    
if __name__ == "__main__":
    # Example usage
    with open("datasources/03-06-2025_14-25.json", "r") as f:
        cleaned_json = remove_trailing_commas(f.read())
        print(f"Cleaned JSON: {cleaned_json}")