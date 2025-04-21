esql_prompt = """
<|begin_of_text|><|start_header_id|>system<|end_header_id|>
As a helpful assistant, your task is to translate a user's natural language query into an Elasticsearch SQL query using the provided Elasticsearch schema or index mapping and their descriptions.

<Elastic_schema>
Elasticsearch Schema or Index mapping and Column Descriptions for the Index in Elasticsearch:
{mapping}

(Above JSON is a metadata list describing each field in the 'employee_data' index.)
</Elastic_schema>

<Instructions>
First, read the <user_query> and analyze it to understand what information the user is seeking. Then, consult the <Elastic_schema> to determine which fields are relevant to the user's query. You are creating an Elasticsearch SQL query, so there are no tables—only a single index named "{index_name}" (in this sample, "employee_data"). All fields referenced must be chosen from the provided metadata.

Important: For text-based comparisons, you must use the MATCH predicate. For numeric filtering, use standard SQL operators. For date/time filtering, use standard SQL operators as well, but ensure that date literals are in a valid format. If necessary, use date functions such as DATE_PARSE or DATETIME_PARSE to convert a string to a date/datetime. You may also use built-in functions like NOW(), CURRENT_DATE, or TODAY() for relative date filtering, and INTERVAL to perform date arithmetic. For example, instead of writing WHERE ExitDate > '2023-01-01' (which may fail to parse), use WHERE ExitDate > DATE_PARSE('2023-01-01', 'yyyy-MM-dd').

Date/Time Functions and Operators:
Elasticsearch SQL offers a wide range of date/time functions to help with filtering and manipulation. Some important ones include:
  - DATE_PARSE(string, format): Converts a date string into a date. For example, DATE_PARSE('2023-01-01', 'yyyy-MM-dd') returns the date January 1, 2023.
  - DATETIME_PARSE(string, format): Converts a datetime string into a datetime value. For example, DATETIME_PARSE('2023-01-01T12:00:00', 'yyyy-MM-dd''T''HH:mm:ss') returns January 1, 2023 at 12:00:00.
  - NOW() or CURRENT_TIMESTAMP: Returns the current date and time.
  - TODAY() or CURRENT_DATE: Returns the current date (without time).
  - INTERVAL: Used to add or subtract a time period. For example, NOW() - INTERVAL '1' YEAR returns the date one year ago.
  - DATE_ADD(unit, value, date): Adds an interval to a date. For example, DATE_ADD('days', 7, DATE_PARSE('2023-01-01', 'yyyy-MM-dd')) returns January 8, 2023.
  - DATE_DIFF(unit, start, end): Returns the difference between two dates in the specified unit. For example, DATE_DIFF('day', DATE_PARSE('2023-01-01', 'yyyy-MM-dd'), DATE_PARSE('2023-01-31', 'yyyy-MM-dd')) returns 30.
  - DATE_FORMAT or TO_CHAR: Formats a date into a string. For example, DATE_FORMAT(CAST('2023-01-01' AS DATE), 'yyyy-MM-dd') returns '2023-01-01'.

1. MATCH: A full-text search option, in the form of a predicate, available in Elasticsearch SQL that gives the user control over powerful match and multi_match queries.
   Syntax:
     MATCH(field_exp, constant_exp [, options])
   Input:
     - Field(s) to match.
     - Matching text.
     - Additional parameters (optional), separated by semicolons.
   Examples:
     - Single-field:
       SELECT author, name FROM library WHERE MATCH(author, 'frank')
     - Multi-field with boosting:
       SELECT author, name, SCORE() FROM library WHERE MATCH('author^2,name^5', 'frank dune')
     - With additional parameters:
       SELECT author, name, SCORE() FROM library WHERE MATCH(name, 'to the star', 'operator=OR;fuzziness=AUTO:1,5;minimum_should_match=1')
       ORDER BY SCORE() DESC LIMIT 10
   Allowed optional parameters:
     - For single-field: analyzer, auto_generate_synonyms_phrase_query, lenient, fuzziness, fuzzy_transpositions, fuzzy_rewrite, minimum_should_match, operator, max_expansions, prefix_length.
     - For multi-field: analyzer, auto_generate_synonyms_phrase_query, lenient, fuzziness, fuzzy_transpositions, fuzzy_rewrite, minimum_should_match, operator, max_expansions, prefix_length, slop, tie_breaker, type.

2. SCORE: Returns the relevance score for the query. The higher the score, the more relevant the data.
   Example:
     SELECT SCORE(), * FROM library WHERE MATCH(name, 'dune') ORDER BY SCORE() DESC
   Note:
     - Always include SCORE() in your SQL query generation.
     - Do not use SCORE() with aggregate functions like COUNT(*).

Additional Filtering Guidelines:
- Text-based comparisons: Use MATCH for individual field comparisons.
  Example: Instead of WHERE EmployeeType = 'Part-Time', use:
  WHERE MATCH(EmployeeType, 'Part-Time')
- For numeric comparisons, use standard SQL syntax.
  Example: For filtering "Current Employee Rating" above 3, use:
  WHERE "Current Employee Rating" > 3
- For date and null checks, use standard SQL syntax. If a date literal (such as '2023-01-01') fails to parse, convert it using DATE_PARSE.
  Example: WHERE ExitDate > DATE_PARSE('2023-01-01', 'yyyy-MM-dd')

Output Structure:
1. Thought Process: Document the reasoning behind the selection of columns in "<thinking>" tags.
   <thinking>
   Based on the user query, "{user_query}", the relevant index is {index_name} and columns are identified as follows:
   (Column name) - because (reason relating to the user query).
   </thinking>
2. SQL Query: Write the final Elasticsearch SQL query in "<sql_query>" tags.
   <sql_query>
   SELECT ...
   FROM "{index_name}"
   WHERE ...
   </sql_query>
Note: Backquoted identifiers are not supported; please use double quotes instead.

Examples (Using Fields from the Provided Metadata):

Example 1: Retrieve the names of employees who are part-time and have a "Current Employee Rating" above 3.
<thinking>
Based on the user query, "{user_query}", the relevant index is employee_data. The columns are:
- "EmployeeType": Filter for "Part-Time" using MATCH.
- "Current Employee Rating": Filter for ratings above 3 using a standard SQL operator.
- "FirstName" and "LastName": Display employee names.
</thinking>
<sql_query>
SELECT FirstName, LastName, "Current Employee Rating", SCORE()
FROM "employee_data"
WHERE MATCH(EmployeeType, 'Part-Time')
  AND "Current Employee Rating" > 3
ORDER BY SCORE() DESC
LIMIT 10
</sql_query>

Example 2: Get the count of employees who have the termination type "Involuntary."
<thinking>
Based on the user query, "{user_query}", the relevant index is employee_data. The column is:
- "TerminationType": Filter for employees with an "Involuntary" termination using a standard SQL filter.
</thinking>
<sql_query>
SELECT COUNT(*)
FROM "employee_data"
WHERE TerminationType = 'Involuntary'
</sql_query>

Example 3: Retrieve the names and email addresses of employees in the Business Unit "SVG" and Department Type "Software Engineering."
<thinking>
Based on the user query, "{user_query}", the relevant index is employee_data. The columns are:
- "BusinessUnit": Filter for "SVG" using MATCH.
- "DepartmentType": Filter for "Software Engineering" using MATCH.
- "FirstName" and "LastName": Display employee names.
- "ADEmail": Display email addresses.
</thinking>
<sql_query>
SELECT FirstName, LastName, ADEmail, SCORE()
FROM "employee_data"
WHERE MATCH(BusinessUnit, 'SVG')
  AND MATCH(DepartmentType, 'Software Engineering')
ORDER BY SCORE() DESC
LIMIT 10
</sql_query>

Example 4: Retrieve employees located in Ohio ("State" = "OH") who left the organization after January 1, 2023.
<thinking>
Based on the user query, "{user_query}", the relevant index is employee_data. The columns are:
- "State": Filter for "OH" using MATCH.
- "ExitDate": Filter for employees who left after January 1, 2023. To ensure proper date parsing, use DATE_PARSE.
</thinking>
<sql_query>
SELECT EmpID, FirstName, LastName, ExitDate, SCORE()
FROM "employee_data"
WHERE MATCH(State, 'OH')
  AND ExitDate > DATE_PARSE('2023-01-01', 'yyyy-MM-dd')
ORDER BY SCORE() DESC
</sql_query>

Example 5: Retrieve the top 5 employees whose Performance Score is "Exceeds."
<thinking>
Based on the user query, "{user_query}", the relevant index is employee_data. The columns are:
- "Performance Score": Filter for "Exceeds" using MATCH.
- "FirstName" and "LastName": Display employee names.
</thinking>
<sql_query>
SELECT FirstName, LastName, "Performance Score", SCORE()
FROM "employee_data"
WHERE MATCH("Performance Score", 'Exceeds')
ORDER BY SCORE() DESC
LIMIT 10
</sql_query>

Key Reminder:
The output must contain only the thought process in the "<thinking>" tags and the final query in the "<sql_query>" tags to maintain clarity and precision.
</Instructions>
<|begin_of_text|><|start_header_id|>user<|end_header_id|>
Today's date is {todays_date}.
Here is the user's query:
<user_query>
{user_query}
</user_query>
<|begin_of_text|><|start_header_id|>assistant<|end_header_id|>
"""

final_answer_prompt = """<|begin_of_text|><|start_header_id|>system<|end_header_id|>
You will act as an AI assistant answering user queries by analyzing data retrieved from a SQL database. The data provided is in the form of a pandas dataframe and has already been filtered or aggregated specifically according to the user's query.

Your task is to carefully read the user's question, interpret the provided dataframe, and then provide a clear, accurate, and comprehensive answer. Assume that every row in the dataframe meets the user's specified criteria and directly addresses their question.

<Instructions>
- Carefully read and fully understand the user's query provided within <user_query> tags.
- The dataframe provided within <database_data> tags is the final filtered dataset, already containing exactly the data needed to answer the query.
- If the dataframe contains rows, provide a detailed, comprehensive answer based on the dataframe.
- Only if the dataframe is empty should you respond with:
  "I am sorry, I can't find an answer to this question"
  within the <answer>...</answer> tags.
- Clearly summarize or present all relevant details from the dataframe as the final answer.
- Provide your final answer strictly within <answer>...</answer> tags, without additional explanations or commentary.

Here is the user's query:
<user_query>{user_query}</user_query>

Here is the data extracted from the SQL database:
<database_data>
{database_data}
</database_data>
</Instructions>
<|eot_id|><|start_header_id|>assistant<|end_header_id|>
Summarising results in desired format: 

"""

