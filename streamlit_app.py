import streamlit as st
import os
import json
import pandas as pd
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from watsonx_wrapper import WatsonxWrapper
from prompts import esql_prompt, final_answer_prompt
from ibm_watsonx_ai.foundation_models.schema import TextGenParameters, TextGenDecodingMethod
from datetime import datetime
import re

# Load environment variables
def load_env():
    load_dotenv()
    return {
        "ES_URL": os.getenv('ELASTIC_URL'),
        "ES_User_Name": os.getenv('ELASTIC_USERNAME'),
        "ES_Password": os.getenv('ELASTIC_PASSWORD')
    }

env = load_env()

# Elasticsearch client
es_client = Elasticsearch(
    env["ES_URL"],
    basic_auth=(env["ES_User_Name"], env["ES_Password"]),
    verify_certs=False
)

# Watsonx client initialization
watsonx_client = WatsonxWrapper(
    model_id="meta-llama/llama-3-3-70b-instruct",
    params=TextGenParameters(
        decoding_method=TextGenDecodingMethod.GREEDY,
        max_new_tokens=1000,
        min_new_tokens=1
    )
)

# Load index mapping
with open("./data/metadata.json", "r") as f:
    mapping = json.load(f)

# Utility functions
def extract_tag(text: str, tag: str = "sql_query") -> str:
    match = re.search(fr"<{tag}>\s*(.*?)\s*</{tag}>", text, re.DOTALL)
    return match.group(1) if match else ""

def execute_esql_query(query: str) -> pd.DataFrame:
    response = es_client.sql.query(body={"query": query}, fetch_size=10000)
    cols = [col['name'] for col in response['columns']]
    return pd.DataFrame(response['rows'], columns=cols)

def generate_esql(question: str) -> str:
    formatted = esql_prompt.format(
        user_query=question,
        index_name="employee_data",
        todays_date=datetime.now().strftime("%Y-%m-%d"),
        mapping=json.dumps(mapping, indent=1)
    )
    response = watsonx_client.generate_text(formatted)
    return extract_tag(response, tag="sql_query")

def summarize_results(question: str, df: pd.DataFrame) -> str:
    formatted = final_answer_prompt.format(
        user_query=question,
        database_data=df.head(20).to_json(orient='records', indent=1)
    )
    response = watsonx_client.generate_text(formatted)
    return extract_tag(response, tag="answer")

# Streamlit UI
st.set_page_config(layout="wide", page_title="Elasticsearch SQL Query App")
st.title("Natural Language to Elasticsearch SQL")

# Predefined questions
predefined_questions = [
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

selected_question = st.selectbox("Choose a predefined question (optional):", ["Enter your own question"] + predefined_questions)

if selected_question == "Enter your own question":
    question = st.text_input("Enter your question:")
else:
    question = st.text_input("Selected question:", selected_question)

if st.button("Generate and Execute"):
    if not question.strip():
        st.error("Please enter a question.")
    else:
        with st.spinner("Generating query..."):
            query = generate_esql(question)

        st.subheader("Generated Elasticsearch SQL Query")
        st.code(query, language='sql')

        if query:
            with st.spinner("Executing query..."):
                df = execute_esql_query(query)

            if not df.empty:
                st.subheader("Query Results")
                st.dataframe(df, use_container_width=True)

                with st.spinner("Summarizing results..."):
                    summary = summarize_results(question, df)

                st.subheader("Summary of Results")
                st.info(summary)
            else:
                st.warning("No results found.")
        else:
            st.error("Failed to generate a valid query.")