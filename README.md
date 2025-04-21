# Elasticsearch SQL & Watsonx: A Comprehensive End-to-End Guide
<p><strong>watsonx + Elastic</strong></p>
<div style="display: flex; align-items: center; gap: 20px;">
  <div style="text-align: center;">
    <img src="./assets/watsonx.png" width="400" height="200">
  </div>
  <div style="text-align: center;">
    <img src="./assets/elastic.jpg" width="400" height="200">
  </div>
</div>


## Scope

Ever wondered how to tap into the power of Elasticsearch without wrestling with its entire Query DSL? Or how to let non-technical teammates ask questions like, “Which employees left this year?” and automatically run the right Elasticsearch SQL? In this comprehensive guide, you’ll learn exactly how to do both—combining the simplicity of SQL and the intelligence of Watsonx to handle everything from basic lookups to advanced text searches.

check the full [guide](./guide.md)

## Usage

* [LICENSE](LICENSE)
* [README.md](README.md)
* [CONTRIBUTING.md](CONTRIBUTING.md)
* [MAINTAINERS.md](MAINTAINERS.md)
* [CHANGELOG.md](CHANGELOG.md)


## Prerequisites

### clone the repo

```bash
git clone <placeholder for public repo>
```

### Elastic Search Setup 

####  **Use Watsonx Discovery**
- follow this [guide](https://github.com/watson-developer-cloud/assistant-toolkit/blob/master/integrations/extensions/docs/elasticsearch-install-and-setup/watsonx_discovery_install_and_setup.md) to setup watsonx discovery to get the DB and kibana.
- we need following credentials in our `.env`
```bash
ELASTIC_URL=""
ELASTIC_USERNAME=""
ELASTIC_PASSWORD=""
```

### **Connecting to watsonx.ai for Natural Language to Elastic SQL Generation**

- Connect with watsonx.ai with following the steps [here](https://dataplatform.cloud.ibm.com/docs/content/wsj/getting-started/get-started-wdp.html?context=wx&audience=wdp)
- Learn more about using watsonx.ai about [here](https://developer.ibm.com/tutorials/awb-text-to-sql-using-llms/)

- we need following credentials in our `.env`
```bash
IBM_CLOUD_API_KEY=""
WATSONX_ENDPOINT=""
WATSONX_PROJECT_ID=""
```
### Download Data

For this walkthrough, we use the [Kaggle Employee Dataset](https://www.kaggle.com/datasets/ravindrasinghrana/employeedataset).


### Setup python envinorment

1. Create a virtual environment:
   ```bash
   python3.11 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```


Before we begine let's Learn about Elasticsearch SQL

## Introduction to Elasticsearch SQL

### What is Elasticsearch SQL?

**Elasticsearch SQL** provides an SQL-based interface to query Elasticsearch data. It allows querying Elasticsearch indices as if they were traditional database tables, enabling users familiar with SQL to leverage Elasticsearch without needing to master its native Query DSL syntax ([Elasticsearch SQL Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/xpack-sql.html)).

### Key Capabilities:

- **SQL Compatibility:** Supports standard SQL operations like `SELECT`, `WHERE`, `GROUP BY`, and aggregation functions.
- **Indexing and Metadata:** Elasticsearch indexes serve as tables, while documents act as rows. Metadata dictionaries (`SHOW TABLES`, `DESCRIBE`) allow schema exploration ([Metadata Commands](https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-commands.html)).
- **Date Handling:** Date fields support various functions (`YEAR()`, `MONTH()`, `DATE_TRUNC()`, etc.), providing flexible date manipulation ([Date Functions](https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-datetime.html)).
- **Full-text Search:** Special functions like `MATCH()` enable powerful text search, integrating Elasticsearch's full-text capabilities within SQL queries ([Match Queries](https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-search.html)).

### Real-world Use Cases:

- **Search Applications:** Quickly implement search functionality using SQL syntax, allowing fast and intuitive development.
- **Analytics and Reporting:** Easily aggregate, analyze, and visualize large datasets by integrating Elasticsearch SQL with Business Intelligence (BI) tools ([Analytics with Elasticsearch SQL](https://www.elastic.co/blog/how-to-use-elasticsearch-sql-for-reporting-and-analytics)).

### ES Vs Traditional SQL

Traditional SQL queries are designed primarily for structured, relational data and rely on fixed schemas and exact-match logic (or basic pattern matching with LIKE). In contrast, Elasticsearch queries harness advanced text analysis, tokenization, and relevance scoring, making them far more flexible and efficient when handling unstructured or semi-structured data. With features like fuzzy matching, proximity and span queries, and dynamic query templating, Elasticsearch provides a richer, more nuanced search experience—enabling rapid, distributed searches across massive datasets that traditional SQL simply cannot match. This combination of power, flexibility, and scalability makes Elasticsearch queries significantly better suited for modern search and analytics use cases.

### Why ElasticSearch SQL and not ElasticSearch Query DSL

We are relying on LLMs to generate the queries from Natural Language, SQL syntax is readily available in the training dataset of these models, we can expect higher accuracy for a wide range of queries.


## Steps

#### Step 1. Indexing Employee Data in Elasticsearch

In the [Indexing code](./indexing.py)
1. Load environment variables (e.g., Elasticsearch credentials).  
2. Create an Elasticsearch client.  
3. Read the **employee_data.csv**, format date columns, and convert them to ISO 8601.  
4. Save the formatted data to both CSV and JSON.  
5. Define an index mapping for our `employee_data` index in Elasticsearch.  
6. Create the index if it doesn’t already exist.  
7. Bulk-load the data from the JSON file into Elasticsearch.

#### Step 2: Watsonx Wrapper Class for LLM Inference

We have added Python class wrapper (`WatsonxWrapper`) that handles Watsonx LLM calls, loading environment variables for the credentials, and abstracting away the text generation and streaming logic. It includes synchronous inference `generate_text` and streaming inference `generate_text_stream`.
[watsonx wrapper class](./watsonx_wrapper.py).

This class will be our main interface to query the LLM for natural language to ES SQL conversion, as well as for automated metadata generation.

#### Step 3. Automating Metadata Creation for Elasticsearch Fields

To make the our Natural Language to elastic SQL, we need create a metadata dictionary of out index, so that the LLM can generate elastic SQL. We’ll use Watsonx to generate natural language descriptions of each field, along with sample values, data types, etc. This is helpful for:

- Documenting our data automatically.
- Building data catalogs or self-service analytics tools.
[Creating Metdata Dictionary](./create_metadata_dictionary.py)

#### Step 4. Natural Language to Elasticsearch SQL with Watsonx

After indexing, we can now harness Watsonx to interpret natural language queries and convert them into ES SQL. We then execute the generated SQL queries against Elasticsearch, returning the results in a tabular form (using `pandas` DataFrames).

code: [nl2eql](./nl2esql.py)
1. Load environment variables and set up the `Elasticsearch` client.  
2. Import the WatsonxWrapper, custom prompts, and parameters.  
3. Define a list of questions.  
4. Generate ES SQL using the Watsonx model.  
5. Execute the SQL and display the results.

Checkout the prompts used [here](./prompts.py)

#### Step 5. run the demo

We have created a simple streamlit app where you can test the results interactively

```bash
streamlit run streamlit_app.py
```

Yohooo!!!!, Celebrate your journey upto this point.

## Summary

So, to summarise, following this guide you will be able to do Advance Retreival Augmeneted Generated with the power of watsonx and Elastic SQL.



To Dive deeper into more elastic search sql features, check out [elasticsearch_queries](./markdowns/queries.md)



## License

If you would like to see the detailed LICENSE click [here](LICENSE).

```txt
# Copyright IBM Corp. {2025}
# SPDX-License-Identifier: Apache-2.
```


## Authors

- [Aditya Mahakali](https://github.com/ADITYAMAHAKALI/)
- [Mohith Charan](https://github.com/Mohit041)

