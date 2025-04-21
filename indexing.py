# %%
import os
import json
import warnings
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd
from elasticsearch import Elasticsearch, helpers, RequestError
from dotenv import load_dotenv

warnings.filterwarnings('ignore')

# %%
def load_environment_vars() -> Dict[str, str]:
    """
    Loads environment variables from the parent directory .env file.

    Returns:
        Dict[str, str]: Dictionary with Elasticsearch URL, username, and password.
    """
    parent_dir: str = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    dotenv_path: str = os.path.join(parent_dir, '.env')
    load_dotenv(dotenv_path)
    return {
        "ES_URL": os.getenv('ELASTIC_URL'),
        "ES_User_Name": os.getenv('ELASTIC_USERNAME'),
        "ES_Password": os.getenv('ELASTIC_PASSWORD')
    }

env_vars: Dict[str, str] = load_environment_vars()

# %%
# Create a global client connection to Elasticsearch
es_client: Elasticsearch = Elasticsearch(
    env_vars["ES_URL"], 
    basic_auth=(env_vars["ES_User_Name"], env_vars["ES_Password"]),
    verify_certs=False,
    request_timeout=10000
)
es_client.info()

# %%
def read_and_format_dates(csv_path: str, date_columns: List[str]) -> pd.DataFrame:
    """
    Reads a CSV file, converts specified date columns from formats like '10-Mar-23'
    to ISO 8601 (YYYY-MM-DDTHH:mm:ss) and fills missing values with 'N/A'.

    Args:
        csv_path (str): Path to the CSV file.
        date_columns (List[str]): List of column names containing date values.

    Returns:
        pd.DataFrame: DataFrame with date columns formatted to ISO 8601.
    """
    try:
        df: pd.DataFrame = pd.read_csv(csv_path)
    except Exception as e:
        raise ValueError(f"Error reading CSV file: {e}")
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S").fillna("N/A")
    return df.fillna("N/A")

csv_file: str = "elastic_data/employee_data.csv"  
date_cols: List[str] = ["StartDate", "ExitDate", "DOB"]
df_formatted: pd.DataFrame = read_and_format_dates(csv_file, date_cols)
df_formatted.to_csv("employees_formatted.csv", index=False)
df_formatted.to_json("employees_formatted.json", orient="records", lines=True)
print("Date formatting completed and files saved.")

# %%
index_mapping: Dict[str, Any] = {
  "settings": {
    "number_of_replicas": 0,
    "number_of_shards": 1,
    "refresh_interval": "1m"
  },
  "mappings": {
    "properties": {
      "EmpID": {"type": "integer"},
      "FirstName": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
      },
      "LastName": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
      },
      "StartDate": {
        "type": "date",
        "format": "yyyy-MM-dd'T'HH:mm:ss||strict_date_time||epoch_millis",
        "ignore_malformed": True
      },
      "ExitDate": {
        "type": "date",
        "format": "yyyy-MM-dd'T'HH:mm:ss||strict_date_time||epoch_millis",
        "ignore_malformed": True
      },
      "Title": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
      },
      "Supervisor": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
      },
      "ADEmail": {"type": "keyword"},
      "BusinessUnit": {"type": "keyword"},
      "EmployeeStatus": {"type": "keyword"},
      "EmployeeType": {"type": "keyword"},
      "PayZone": {"type": "keyword"},
      "EmployeeClassificationType": {"type": "keyword"},
      "TerminationType": {"type": "keyword"},
      "TerminationDescription": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
      },
      "DepartmentType": {"type": "keyword"},
      "Division": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
      },
      "DOB": {
        "type": "date",
        "format": "yyyy-MM-dd'T'HH:mm:ss||strict_date_time||epoch_millis",
        "ignore_malformed": True
      },
      "State": {"type": "keyword"},
      "JobFunctionDescription": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
      },
      "GenderCode": {"type": "keyword"},
      "LocationCode": {"type": "integer"},
      "RaceDesc": {"type": "keyword"},
      "MaritalDesc": {"type": "keyword"},
      "Performance Score": {"type": "keyword"},
      "Current Employee Rating": {"type": "integer"}
    }
  }
}

# %%
def create_index(index_name: str, mapping: Dict[str, Any]) -> None:
    """
    Creates an Elasticsearch index with the given mapping. Prints a message on success
    or if the index already exists.

    Args:
        index_name (str): Name of the Elasticsearch index.
        mapping (Dict[str, Any]): Mapping configuration for the index.
    """
    try:
        es_client.indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created successfully.")
    except RequestError as e:
        if e.error == 'resource_already_exists_exception':
            print(f"Index '{index_name}' already exists.")
        else:
            print(f"An error occurred while creating index '{index_name}': {e}")

index_name: str = "employee_data"
create_index(index_name, index_mapping)

# %%
def load_json_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Loads JSON documents from a newline-delimited JSON file.

    Args:
        file_path (str): Path to the JSON file.

    Returns:
        List[Dict[str, Any]]: List of JSON documents.
    """
    documents: List[Dict[str, Any]] = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    documents.append(json.loads(line))
    except Exception as e:
        raise ValueError(f"Error reading JSON file: {e}")
    return documents

# %%
def index_documents(index_name: str, docs: List[Dict[str, Any]]) -> None:
    """
    Bulk indexes JSON documents into the specified Elasticsearch index.

    Args:
        index_name (str): Target index name.
        docs (List[Dict[str, Any]]): List of JSON documents.
    """
    actions: List[Dict[str, Any]] = [{"_index": index_name, "_source": doc} for doc in docs]
    try:
        helpers.bulk(es_client, actions)
        print(f"Indexed {len(docs)} documents into index '{index_name}'.")
    except Exception as e:
        print(f"Error indexing documents: {e}")

json_file: str = "employees_formatted.json"
documents: List[Dict[str, Any]] = load_json_file(json_file)
index_documents(index_name, documents)


