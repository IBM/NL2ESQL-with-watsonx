# %% [markdown]
# 
# This notebook demonstrates how to generate Elasticsearch SQL queries from natural language questions
# using the WatsonxWrapper for large language model inference, then execute those queries against an
# Elasticsearch cluster and display the results as a pandas DataFrame.
# 

# %%
import os
import json
import warnings
from datetime import datetime
from typing import List, Dict, Any
from IPython.display import display
import pandas as pd
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# We will ignore some warnings for cleaner output
warnings.filterwarnings('ignore')

# %%
# 1. Load environment variables from the parent directory's .env file.

def load_environment_vars() -> Dict[str, str]:
    """
    Loads environment variables from the parent directory .env file.
    
    Returns:
        Dict[str, str]: Dictionary with Elasticsearch connection details.
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
# 2. Create a global Elasticsearch client connection.

es_client: Elasticsearch = Elasticsearch(
    env_vars["ES_URL"],
    basic_auth=(env_vars["ES_User_Name"], env_vars["ES_Password"]),
    verify_certs=False,
    request_timeout=10000
)

# Check basic info to confirm connection
es_info = es_client.info()
print("Connected to Elasticsearch:")
print(es_info)

# %%
# 3. Import the WatsonxWrapper and necessary classes to handle text generation.

from watsonx_wrapper import WatsonxWrapper
from prompts import esql_prompt
from ibm_watsonx_ai.foundation_models.schema import TextGenParameters, TextGenDecodingMethod

# %%
"""
Below is our list of 20 natural language questions about employees. We'll pass each question
through the large language model to generate an Elasticsearch SQL query, and then execute that query.
"""

questions: List[str] = [
    "Which employees are part-time and have a Current Employee Rating above 3?",
    "Show me all employees with a termination type of 'Involuntary'.",
    "How many active employees are currently in 'Finance & Accounting'?",
    "Who in 'Admin Offices' has 'Manager' in their JobFunctionDescription?",
    "Which employees have a 'Voluntary' termination type?",
    "Retrieve employees whose LocationCode is greater than 30000 and are Full-Time.",
    "Show employees in the 'SVG' BusinessUnit with 'Exceeds' performance.",
    "Which employees have an ExitDate not null and are still marked 'Active'?",
    "List the top 5 employees by SCORE() in the 'WBL' BusinessUnit.",
    "How many employees have a DOB before 1980 in 'IT/IS' department?",
    "Get all employees whose MaritalDesc is 'Married' and State is 'NY'.",
    "Which employees have a PayZone of 'Zone A' or 'Zone B'?",
    "Find employees in 'EW' BusinessUnit who started after 2021-01-01.",
    "Which employees have 'Temporary' EmployeeClassificationType in 'Executive Office'?",
    "Show employees in 'IT Support' or 'Data Analyst' roles who are still Active.",
    "How many employees in the 'Admin Offices' are Female with a rating above 4?",
    "Retrieve all employees in 'Sales' who have an ExitDate after 1680307200000."
]

# %%
# 4. Create a Watsonx client for text generation.

watsonx_client: WatsonxWrapper = WatsonxWrapper(
    model_id="meta-llama/llama-3-3-70b-instruct",
    params=TextGenParameters(
        decoding_method=TextGenDecodingMethod.GREEDY,
        max_new_tokens=1000,
        min_new_tokens=1
    )
)

# %%

todays_date: str = datetime.now().strftime("%Y-%m-%d")
todays_date

# %%
# 5. Load mapping (metadata about the Elasticsearch fields) from a local JSON file.

todays_date: str = datetime.now().strftime("%Y-%m-%d")
index_name: str = "employee_data"

with open("./metadata.json", "r") as f:
    mapping: Any = json.load(f)

# %%
def extract_tag(text: str, tag: str = "sql_query") -> str:
    """
    Extracts content enclosed within <tag> and </tag> from the given text.
    
    Args:
        text (str): The text to parse.
        tag  (str): The tag name to extract (default: 'sql_query').

    Returns:
        str: Extracted string or empty if not found.
    """
    import re
    match = re.search(fr"<{tag}>\s*(.*?)\s*</{tag}>", text, re.DOTALL)
    return match.group(1) if match else ""

# %%
def execute_esql_query(query: str) -> pd.DataFrame:
    """
    Executes the given Elasticsearch SQL query and returns a pandas DataFrame.

    Args:
        query (str): The Elasticsearch SQL query.

    Returns:
        pd.DataFrame: The query results as a DataFrame.
    """
    response = es_client.sql.query(body={"query": query}, fetch_size=10000)
    cols = [col['name'] for col in response['columns']]
    df = pd.DataFrame(response['rows'], columns=cols)
    return df

# %%
def generate_esql(question: str) -> str:
    """
    Generates an Elasticsearch SQL query from a natural language question
    using the watsonx_client and the pre-defined esql_prompt.

    Args:
        question (str): The user's natural language question.

    Returns:
        str: The generated Elasticsearch SQL query.
    """
    formatted = esql_prompt.format(
        user_query=question,
        index_name=index_name,
        todays_date=todays_date,
        mapping=json.dumps(mapping, indent=1)
    )
    response = watsonx_client.generate_text(formatted)
    return extract_tag(response, tag="sql_query")

# %%
def run_all_questions(questions: List[str]) -> None:
    """
    Iterates through all questions, generates and executes each query,
    and displays the result DataFrame.
    
    Args:
        questions (List[str]): List of natural language questions.
    """
    for idx, question in enumerate(questions, start=1):
        print(f"\n{'-'*60}")
        print(f"Question {idx}: {question}")
        query = generate_esql(question)
        print(f"\nGenerated SQL Query:\n{query}\n")
        if query.strip():
            df = execute_esql_query(query)
            if df.empty:
                print("No results found.")
            else:
                print("Query Results:")
                display(df)  # Display top 5 rows for brevity
        else:
            print("No query generated or query is empty.")

# %%
"""
## Running All Questions
Below, we call `run_all_questions(questions)` to automatically generate and execute queries
for each of the 20 questions and display the first few results.
"""

# %%
run_all_questions(questions)


