# %%
import json
from typing import Dict, Any
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ElasticsearchException
import os
from dotenv import load_dotenv

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

def get_es_client() -> Elasticsearch:
    """
    Create and return an Elasticsearch client instance using environment variables.

    Returns:
        Elasticsearch: The Elasticsearch client.
    
    Raises:
        ConnectionError: If unable to connect to Elasticsearch.
    """
    env_vars: Dict[str, str] = load_environment_vars()
    try:
        es = Elasticsearch(
            env_vars["ES_URL"],
            basic_auth=(env_vars["ES_User_Name"], env_vars["ES_Password"]),
            verify_certs=False,
            request_timeout=10000
        )
        es.info()  # Ensure the connection works
        return es
    except Exception as ex:
        raise ConnectionError(f"Error connecting to Elasticsearch: {ex}")

def execute_query(es: Elasticsearch, index: str, query: Dict[str, Any]) -> None:
    """
    Execute a search query on the specified Elasticsearch index and print the response.

    Args:
        es (Elasticsearch): Elasticsearch client instance.
        index (str): Name of the index.
        query (Dict[str, Any]): Query DSL as a dictionary.
    """
    try:
        response = es.search(index=index, body=query)
        print(json.dumps(response, indent=2))
    except ElasticsearchException as err:
        print(f"Elasticsearch error on index '{index}': {err}")

es_client: Elasticsearch = get_es_client()

# %% [markdown]
# ### Full-Text Queries

# %%
# Match Query: Find employees whose JobFunctionDescription contains "Engineer"
query_match = {
    "query": {
        "match": {
            "JobFunctionDescription": {
                "query": "Engineer",
                "operator": "or"
            }
        }
    }
}
execute_query(es_client, "employee_data", query_match)

# %%
# Match Phrase Query: Retrieve employees with Title exactly "Senior BI Developer"
query_match_phrase = {
    "query": {
        "match_phrase": {
            "Title": "Senior BI Developer"
        }
    }
}
execute_query(es_client, "employee_data", query_match_phrase)

# %%
# Match Phrase Prefix Query: Support searching for a title starting with "Senior BI Dev"
query_match_phrase_prefix = {
    "query": {
        "match_phrase_prefix": {
            "Title": "Senior BI Dev"
        }
    }
}
execute_query(es_client, "employee_data", query_match_phrase_prefix)

# %%
# Match Bool Prefix Query: Autocomplete scenario on the Title field
query_match_bool_prefix = {
    "query": {
        "match_bool_prefix": {
            "Title": {
                "query": "Senior BI Dev",
                "operator": "and"
            }
        }
    }
}
execute_query(es_client, "employee_data", query_match_bool_prefix)

# %%
# Combined Fields Query: Search for "Senior Developer" across Title, JobFunctionDescription, and Division
query_combined_fields = {
    "query": {
        "combined_fields": {
            "query": "Senior Developer",
            "fields": ["Title^3", "JobFunctionDescription", "Division"],
            "operator": "and"
        }
    }
}
execute_query(es_client, "employee_data", query_combined_fields)

# %%
# Multi-Match Query: Search for "Senior Developer" in multiple fields with boosting on Title
query_multi_match = {
    "query": {
        "multi_match": {
            "query": "Senior Developer",
            "fields": ["Title^2", "JobFunctionDescription", "Division"],
            "type": "best_fields"
        }
    }
}
execute_query(es_client, "employee_data", query_multi_match)

# %%
# Query String Query: Search for employees with JobFunctionDescription containing "Developer" but not "Intern"
query_query_string = {
    "query": {
        "query_string": {
            "query": "(Developer AND NOT Intern)",
            "fields": ["JobFunctionDescription"],
            "default_operator": "and",
            "fuzziness": "AUTO",
            "minimum_should_match": "75%",
            "lenient": True
        }
    }
}
execute_query(es_client, "employee_data", query_query_string)

# %%
# Simple Query String Query: Search on JobFunctionDescription and Title using a simplified syntax
query_simple_query_string = {
    "query": {
        "simple_query_string": {
            "query": "(Developer | Engineer) -Intern",
            "fields": ["JobFunctionDescription", "Title"],
            "default_operator": "and"
        }
    }
}
execute_query(es_client, "employee_data", query_simple_query_string)

# %% [markdown]
# ### Term-Level Queries

# %%
# Term Query: Find employees with EmployeeStatus "Active"
query_term = {
    "query": {
        "term": {"EmployeeStatus": "Active"}
    }
}
execute_query(es_client, "employee_data", query_term)

# %%
# Terms Query: Retrieve employees with RaceDesc either "Black" or "White"
query_terms = {
    "query": {
        "terms": {"RaceDesc": ["Black", "White"]}
    }
}
execute_query(es_client, "employee_data", query_terms)

# %%
# Range Query (Numeric): Find employees with Current Employee Rating between 3 and 5
query_range_numeric = {
    "query": {
        "range": {"Current Employee Rating": {"gte": 3, "lte": 5}}
    }
}
execute_query(es_client, "employee_data", query_range_numeric)

# %%
# Range Query (Date): Find employees with DOB greater than a specified Unix timestamp
query_range_date = {
    "query": {
        "range": {"DOB": {"gt": -373593600000}}
    }
}
execute_query(es_client, "employee_data", query_range_date)

# %%
# Exists Query: Find employees who have an ExitDate field
query_exists = {
    "query": {
        "exists": {"field": "ExitDate"}
    }
}
execute_query(es_client, "employee_data", query_exists)

# %%
# Prefix Query: Retrieve employees whose BusinessUnit starts with "S"
query_prefix = {
    "query": {
        "prefix": {"BusinessUnit": "S"}
    }
}
execute_query(es_client, "employee_data", query_prefix)

# %%
# Wildcard Query: Match MaritalDesc values that start with "S" and end with "le"
query_wildcard = {
    "query": {
        "wildcard": {"MaritalDesc": "S*le"}
    }
}
execute_query(es_client, "employee_data", query_wildcard)

# %%
# Regexp Query: Find employees whose FirstName matches the regular expression "A..a"
query_regexp = {
    "query": {
        "regexp": {"FirstName": "A..a"}
    }
}
execute_query(es_client, "employee_data", query_regexp)

# %%
# Fuzzy Query: Find employees with a LastName similar to "Canty" (to catch misspellings)
query_fuzzy = {
    "query": {
        "fuzzy": {"LastName": {"value": "Canty", "fuzziness": "AUTO"}}
    }
}
execute_query(es_client, "employee_data", query_fuzzy)

# %%
# Ids Query: Retrieve specific employees by their IDs
query_ids = {
    "query": {
        "ids": {"values": ["1001", "1002", "1003"]}
    }
}
execute_query(es_client, "employee_data", query_ids)

# %% [markdown]
# ### Span Queries

# %%
# Span Term Query: Find employees whose Title contains the token "engineer"
query_span_term = {
    "query": {
        "span_term": {"Title": "engineer"}
    }
}
execute_query(es_client, "employee_data", query_span_term)

# %%
# Span Multi Query: Find tokens in JobFunctionDescription starting with "manag" (e.g., manager, management)
query_span_multi = {
    "query": {
        "span_multi": {
            "match": {"wildcard": {"JobFunctionDescription": "manag*"}}
        }
    }
}
execute_query(es_client, "employee_data", query_span_multi)

# %%
# Span First Query: Check if the token "data" appears within the first 3 positions of Title
query_span_first = {
    "query": {
        "span_first": {
            "match": {"span_term": {"Title": "data"}},
            "end": 3
        }
    }
}
execute_query(es_client, "employee_data", query_span_first)

# %%
# Span Near Query: Find employees whose Title contains "senior" followed by "developer" within 3 tokens
query_span_near = {
    "query": {
        "span_near": {
            "clauses": [
                {"span_term": {"Title": "senior"}},
                {"span_term": {"Title": "developer"}}
            ],
            "slop": 3,
            "in_order": True
        }
    }
}
execute_query(es_client, "employee_data", query_span_near)

# %%
# Span Or Query: Retrieve employees whose Title contains either "developer" or "engineer"
query_span_or = {
    "query": {
        "span_or": {
            "clauses": [
                {"span_term": {"Title": "developer"}},
                {"span_term": {"Title": "engineer"}}
            ]
        }
    }
}
execute_query(es_client, "employee_data", query_span_or)

# %%
# Span Not Query: Find employees with "manager" in Title but exclude those with "assistant"
query_span_not = {
    "query": {
        "span_not": {
            "include": {"span_term": {"Title": "manager"}},
            "exclude": {"span_term": {"Title": "assistant"}}
        }
    }
}
execute_query(es_client, "employee_data", query_span_not)

# %% [markdown]
# ### Specialized Queries

# %%
# Constant Score Query: Retrieve employees with EmployeeStatus "Active" using a constant score
query_constant_score = {
    "query": {
        "constant_score": {
            "filter": {"term": {"EmployeeStatus": "Active"}},
            "boost": 1.0
        }
    }
}
execute_query(es_client, "employee_data", query_constant_score)

# %%
# Boosting Query: Get employees with JobFunctionDescription containing "Engineer" but demote if classified as Temporary
query_boosting = {
    "query": {
        "boosting": {
            "positive": {"match": {"JobFunctionDescription": "Engineer"}},
            "negative": {"term": {"EmployeeClassificationType": "Temporary"}},
            "negative_boost": 0.5
        }
    }
}
execute_query(es_client, "employee_data", query_boosting)

# %%
# Indices Query: Apply different queries based on the index; example for employee_data vs contractor_data
query_indices = {
    "query": {
        "indices": {
            "indices": ["employee_data"],
            "query": {"term": {"EmployeeStatus": "Active"}},
            "no_match_query": {"term": {"ContractorStatus": "Verified"}}
        }
    }
}
execute_query(es_client, "employee_data", query_indices)

# %%
# More Like This Query: Retrieve employees similar to a sample text based on JobFunctionDescription
query_more_like_this = {
    "query": {
        "more_like_this": {
            "fields": ["JobFunctionDescription"],
            "like": "Experienced Software Engineer specializing in backend systems",
            "min_term_freq": 1,
            "min_doc_freq": 1
        }
    }
}
execute_query(es_client, "employee_data", query_more_like_this)

# %%
# Template Query: Search using a pre-registered template "employee_state_search"
template_query = {
    "id": "employee_state_search",
    "params": {"state_param": "NY"}
}
try:
    response = es_client.search_template(index="employee_data", body=template_query)
    print("Template Query Response:")
    print(json.dumps(response, indent=2))
except ElasticsearchException as err:
    print(f"Template Query error: {err}")

# %%
# Percolate Query Registration: Register a percolator query in the employee_alerts index
percolator_query = {
    "query": {"match": {"TerminationDescription": "data inconsistency"}}
}
try:
    response = es_client.index(index="employee_alerts", id="1", body=percolator_query)
    print("Percolator Query Registered:")
    print(json.dumps(response, indent=2))
except ElasticsearchException as err:
    print(f"Percolator Registration error: {err}")

# %%
# Percolate Query Execution: Percolate a document to find matching registered queries
percolate_query = {
    "query": {
        "percolate": {
            "field": "query",
            "document": {
                "TerminationDescription": "Observed data inconsistency and irregular termination notes."
            }
        }
    }
}
try:
    response = es_client.search(index="employee_alerts", body=percolate_query)
    print("Percolate Query Response:")
    print(json.dumps(response, indent=2))
except ElasticsearchException as err:
    print(f"Percolate Query error: {err}")

# %% [markdown]
# ### Nested / Parent–Child Queries

# %%
# Nested Query: Find employees with a nested skill "Java" and proficiency greater than 3
query_nested = {
    "query": {
        "nested": {
            "path": "Skills",
            "query": {
                "bool": {
                    "must": [
                        {"match": {"Skills.name": "Java"}},
                        {"range": {"Skills.proficiency": {"gt": 3}}}
                    ]
                }
            },
            "score_mode": "avg",
            "inner_hits": {}
        }
    }
}
execute_query(es_client, "employee_data", query_nested)

# %%
# Has Child Query: Find parent documents (posts) with child comments containing "excellent"
query_has_child = {
    "query": {
        "has_child": {
            "type": "comment",
            "query": {"match": {"comment_text": "excellent"}},
            "score_mode": "avg",
            "min_children": 1,
            "inner_hits": {}
        }
    }
}
execute_query(es_client, "employee_data", query_has_child)

# %%
# Has Parent Query: Retrieve child documents (comments) for posts with a title containing "Elasticsearch"
query_has_parent = {
    "query": {
        "has_parent": {
            "parent_type": "post",
            "query": {"match": {"post_title": "Elasticsearch"}},
            "score": True,
            "inner_hits": {}
        }
    }
}
execute_query(es_client, "employee_data", query_has_parent)

# %% [markdown]
# ### Other Queries

# %%
# Match All Query: Fetch all employee documents with a uniform score
query_match_all = {
    "query": {
        "match_all": {"boost": 1.0}
    }
}
execute_query(es_client, "employee_data", query_match_all)
