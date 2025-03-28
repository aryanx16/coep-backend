# src/app/services/_delta_utils.py
import json
import time
import datetime

def _parse_delta_schema_string(schema_string):
    """
    Parses Delta's JSON schema string into an Iceberg-like schema dict.
    Assigns sequential IDs (starting from 1) and maps 'nullable' to 'required'.
    """
    try:
        delta_schema = json.loads(schema_string)
        # Basic validation of the Delta schema structure
        if not isinstance(delta_schema, dict) or \
           delta_schema.get("type") != "struct" or \
           not isinstance(delta_schema.get("fields"), list):
            print(f"Warning: Invalid or unexpected Delta schema structure: {delta_schema}")
            return None # Or raise a specific error

        iceberg_fields = []
        current_id = 1 # Start IDs from 1
        for field in delta_schema["fields"]:
            if not isinstance(field, dict):
                print(f"Warning: Skipping invalid field entry (not a dict): {field}")
                continue

            field_name = field.get("name")
            field_type = field.get("type") # TODO: More complex type mapping might be needed (e.g., struct, array, map)
            nullable = field.get("nullable", True) # Assume nullable if missing

            if not field_name or not field_type:
                print(f"Warning: Skipping field with missing name or type in Delta schema: {field}")
                continue

            iceberg_fields.append({
                "id": current_id,
                "name": field_name,
                "required": not nullable, # Iceberg 'required' is the inverse of Delta 'nullable'
                "type": field_type,
                # Optional: Add documentation if available in metadata
                # "doc": field.get("metadata", {}).get("comment", "")
            })
            current_id += 1

        return {
            "schema-id": 0, # Default schema ID for the main schema
            "type": "struct",
            "fields": iceberg_fields
        }
    except json.JSONDecodeError as e:
        print(f"Error decoding Delta schema JSON string: {e}")
        print(f"Schema string was: {schema_string[:500]}...") # Log snippet
        return None # Or raise
    except Exception as e:
        print(f"Unexpected error parsing Delta schema: {e}")
        traceback.print_exc()
        return None # Or raise