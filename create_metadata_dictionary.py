# %% [markdown]
# ## 1. Imports, Environment, and Elasticsearch Client Setup
# 
# 

# %%
from watsonx_wrapper import WatsonxWrapper
from ibm_watsonx_ai.foundation_models.schema import TextGenParameters, TextGenDecodingMethod
from elasticsearch import Elasticsearch
import json
import re
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from dotenv import load_dotenv

# Get the path to the parent directory and load the .env file
parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
dotenv_path = os.path.join(parent_dir, '.env')
load_dotenv(dotenv_path)

# Access environment variables
ES_URL = os.getenv('ELASTIC_URL')
ES_User_Name = os.getenv('ELASTIC_USERNAME')
ES_Password = os.getenv('ELASTIC_PASSWORD')

# Create a global Elasticsearch client connection
es_client = Elasticsearch(
    ES_URL, 
    basic_auth=(ES_User_Name, ES_Password),
    verify_certs=False,
    request_timeout=10000
)
print(es_client.info())





# %% [markdown]
# ## 2. Define Helper Functions for Index and Field Operations
# We update the field extraction function to return both the field name and its data type.
# 
# 

# %%
def get_indices(es: Elasticsearch) -> List[str]:
    """
    Retrieve all index names from Elasticsearch.

    :param es: Elasticsearch client.
    :return: List of index names.
    """
    return list(es.indices.get_alias().keys())

def get_fields(es: Elasticsearch, index: str) -> List[Dict[str, str]]:
    """
    Retrieve field details from an Elasticsearch index mapping.

    :param es: Elasticsearch client.
    :param index: Index name.
    :return: List of dictionaries with field_name and data_type.
    """
    mapping = es.indices.get_mapping(index=index)
    properties = mapping[index]['mappings'].get('properties', {})
    fields = []
    for field, details in properties.items():
        dtype = details.get('type', 'unknown')
        fields.append({"field_name": field, "data_type": dtype})
    return fields





# %% [markdown]
# ## 3. Define Function to Get Diverse Terms Using Aggregations
# 
# 

# %%
def get_diverse_terms(es: Elasticsearch, index: str, field: str,data_type: str, size: int = 10) -> Dict[str, Any]:
    """
    Retrieve diverse representative terms using multiple aggregations.

    :param es: Elasticsearch client.
    :param index: Index name.
    :param field: Field name.
    :param size: Number of terms to retrieve.
    :return: Dictionary containing various term types.
    """
    # If the field is a text type, use its keyword sub-field (if available)
    agg_field = field + ".keyword" if data_type == "text" else field
    query = {
        "aggs": {
            "frequent_terms": {
                "terms": {
                    "field": agg_field,
                    "size": size
                }
            },
            "rare_terms": {
                "rare_terms": {
                    "field": agg_field,
                    "max_doc_count": 5
                }
            },
            "significant_terms": {
                "significant_terms": {
                    "field": agg_field,
                    "size": size
                }
            },
            "unique_count": {
                "cardinality": {
                    "field": agg_field
                }
            },
            "sample_docs": {
                "top_hits": {
                    "size": 3
                }
            }
        }
    }

    response = es.search(index=index, body=query)
    return {
        "frequent_terms": [bucket["key"] for bucket in response["aggregations"]["frequent_terms"]["buckets"]],
        "rare_terms": [bucket["key"] for bucket in response["aggregations"]["rare_terms"]["buckets"]],
        "significant_terms": [bucket["key"] for bucket in response["aggregations"]["significant_terms"]["buckets"]],
        "unique_count": response["aggregations"]["unique_count"]["value"],
        "sample_docs": response["aggregations"]["sample_docs"]["hits"]["hits"]
    }





# %% [markdown]
# ## 4. Initialize WatsonX Client for Column Description Generation
# 
# 

# %%
watsonx_client = WatsonxWrapper(
    model_id="meta-llama/llama-3-3-70b-instruct",
    params=TextGenParameters(
        decoding_method=TextGenDecodingMethod.GREEDY,
        max_new_tokens=1000,
        min_new_tokens=1
    )
)

# %% [markdown]
# ## 5. Define the Prompt Template and JSON Extraction Function
# We update our prompt to include a personality-driven system message, detailed instructions in the user prompt, and an exact JSON output format.
# 
# 

# %%
LLAMA3_BASE_PROMPT = """<|begin_of_text|><|start_header_id|>system<|end_header_id|>
{system_prompt}<|eot_id|><|start_header_id|>user<|end_header_id|>
{user_prompt}<|eot_id|><|start_header_id|>assistant<|end_header_id|>
{llm_prefix}"""

# Define system prompt: personality and purpose
SYSTEM_PROMPT = (
    "You are a highly knowledgeable metadata dictionary agent. "
    "Your mission is to help Elasticsearch admins understand their data by providing clear, concise natural language descriptions for each field."
)

# Define user prompt template with instructions, input data, and required JSON output format.
USER_PROMPT_TEMPLATE = (
    "Analyze the field based on the input below:\n"
    "- Index Name: {index}\n"
    "- Field Name: {field}\n"
    "- Data Type: {data_type}\n"
    "- Sample Values: {samples}\n\n"
    "Generate a description that explains the field's purpose. "
    "Return the output strictly in the following JSON format (no additional text):\n\n"
    "json\n"
    "{{\n"
    '  "field_name": "{field}",\n'
    '  "index_name": "{index}",\n'
    '  "data_type": "{data_type}",\n'
    '  "natural_language_description": "<your description>",\n'
    '  "sample_value": "{samples}"\n'
    "}}\n"
    ""
)

# Define LLM prefix text
LLM_PREFIX = "**Generating Metadata Dictionary in desired format:**"

def generate_prompt(index: str, field: str, data_type: str, samples: List[str]) -> str:
    """
    Generate the full prompt using the base prompt template.

    :param index: Index name.
    :param field: Field name.
    :param data_type: Data type of the field.
    :param samples: Sample values from the field.
    :return: The complete prompt string.
    """
    user_prompt = USER_PROMPT_TEMPLATE.format(
        index=index,
        field=field,
        data_type=data_type,
        samples=', '.join(map(str, samples))
    )
    return LLAMA3_BASE_PROMPT.format(
        system_prompt=SYSTEM_PROMPT,
        user_prompt=user_prompt,
        llm_prefix=LLM_PREFIX
    )

def extract_json(text: str) -> Dict[str, Any]:
    """
    Extract a JSON object from the text using regex.

    :param text: The text generated by the LLM.
    :return: Parsed JSON as a dictionary.
    :raises ValueError: If no JSON object can be found.
    """
    # This regex finds the first JSON-like structure between curly braces.
    pattern = r"(\{(?:.|\n)*\})"
    match = re.search(pattern, text)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing JSON: {e}")
    raise ValueError("No JSON object found in the text.")

# %% [markdown]
# ## 6. Test for a Single Index and a Single Field
# Here we select a specific index and field, generate the prompt, call the LLM, and then extract the JSON output.
# 
# 

# %%
# Select test index and field details
test_index = 'employee_data'
fields_info = get_fields(es_client, test_index)
if not fields_info:
    raise ValueError(f"No fields found in index {test_index}.")

# Use the first field for testing
test_field_info = fields_info[0]
test_field = test_field_info["field_name"]
data_type = test_field_info["data_type"]

# Get diverse terms for the test field
diverse_terms = get_diverse_terms(es_client, test_index, test_field,data_type)
combined_samples = list(set(
    diverse_terms["frequent_terms"] +
    diverse_terms["rare_terms"] +
    diverse_terms["significant_terms"]
))[:10]  # limit to 10 samples

# Generate the full prompt
prompt = generate_prompt(test_index, test_field, data_type, combined_samples)
print("=== Generated Prompt ===")
print(prompt)

# Call the LLM
llm_output = watsonx_client.generate_text(prompt=prompt).strip()
print("\n=== Raw LLM Output ===")
print(llm_output)

# Extract JSON from the LLM output
try:
    json_output = extract_json(llm_output)
    print("\n=== Extracted JSON ===")
    print(json.dumps(json_output, indent=4))
except ValueError as e:
    print(f"Error extracting JSON: {e}")

# %% [markdown]
# ## 7. Multithreaded Processing Over Fields in an Index
# This cell processes multiple fields concurrently in the selected index.
# 
# 

# %%
def process_field(es: Elasticsearch, index: str, field_detail: Dict[str, str],
                  llm_client: WatsonxWrapper, threshold: int = 20) -> Dict[str, Any]:
    """
    Process a single field from an Elasticsearch index to generate metadata.

    :param es: Elasticsearch client.
    :param index: Index name.
    :param field_detail: Dictionary containing field_name and data_type.
    :param llm_client: WatsonX LLM client.
    :param threshold: Maximum number of sample values to use.
    :return: Dictionary with metadata for the field.
    """
    field = field_detail["field_name"]
    data_type = field_detail["data_type"]
    diverse_terms = get_diverse_terms(es, index, field,data_type=data_type,size=threshold)
    combined_samples = list(set(
        diverse_terms["frequent_terms"] +
        diverse_terms["rare_terms"] +
        diverse_terms["significant_terms"]
    ))[:threshold]
    prompt = generate_prompt(index, field, data_type, combined_samples)
    llm_text = llm_client.generate_text(prompt=prompt).strip()
    
    try:
        result_json = extract_json(llm_text)
    except ValueError:
        result_json = {
            "field_name": field,
            "index_name": index,
            "data_type": data_type,
            "natural_language_description": llm_text,
            "sample_value": combined_samples[0] if combined_samples else ""
        }
    
    return result_json

# Process all fields in a test index concurrently
fields_info = get_fields(es_client, test_index)
metadata_fields: List[Dict[str, Any]] = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(process_field, es_client, test_index, field_info, watsonx_client): field_info for field_info in fields_info}
    for future in as_completed(futures):
        metadata_fields.append(future.result())

print("=== Metadata for each field in index:", test_index, "===")
print(json.dumps(metadata_fields, indent=4))





# %%
with open('metadata.json', 'w') as f:    
    json.dump(metadata_fields, f, indent=4)

# %% [markdown]
# ## 8. Full Metadata Dictionary Generation Over All Indices
# This final cell processes every index using multithreaded field processing.
# 
# 

# %%
def process_index(es: Elasticsearch, index: str, llm_client: Any, threshold: int = 20) -> List[Dict[str, Any]]:
    """
    Process an Elasticsearch index to generate a metadata dictionary for each field.

    :param es: Elasticsearch client.
    :param index: Index name.
    :param llm_client: WatsonX LLM client.
    :param threshold: Maximum number of sample values per field.
    :return: List of field metadata dictionaries.
    """
    fields_info = get_fields(es, index)
    metadata: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_field, es, index, field_info, llm_client, threshold): field_info for field_info in fields_info}
        for future in as_completed(futures):
            metadata.append(future.result())
    return metadata

def generate_metadata_dictionary(es: Elasticsearch, llm_client: Any, threshold: int = 20) -> List[Dict[str, Any]]:
    """
    Generate a full metadata dictionary for all Elasticsearch indices.

    :param es: Elasticsearch client.
    :param llm_client: WatsonX LLM client.
    :param threshold: Maximum number of sample values per field.
    :return: List of metadata entries.
    """
    indices = get_indices(es)
    metadata_dict: List[Dict[str, Any]] = []
    for index in indices:
        metadata_dict.extend(process_index(es, index, llm_client, threshold))
    return metadata_dict

# Generate metadata dictionary for all indices
full_metadata_dict = generate_metadata_dictionary(es_client, watsonx_client)
print("=== Full Metadata Dictionary ===")
print(json.dumps(full_metadata_dict, indent=4))

# Optionally, save the full metadata dictionary to a JSON file
with open("es_full_metadata.json", "w") as file:
    json.dump(full_metadata_dict, file, indent=4)


