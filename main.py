import functions_framework
import pandas as pd
from google.cloud import bigquery
import vertexai
import json
import jsonify
import os


project = os.environ.get('project')
location = os.environ.get('location')
dataset = os.environ.get('dataset')
table = os.environ.get('table')
schema = os.environ.get('schema')

from vertexai.language_models import CodeGenerationModel
from vertexai.preview.generative_models import GenerativeModel, Part

# function that accepts dialogflow request which includes prompt


@functions_framework.http
def handle_webhook(request):
    # parse request into JSON
    req = request.get_json()

    # we set the session parameter name in dialogflow as question
    question = req["sessionInfo"]["parameters"]['question']
    print(question)

    output = create_sql(question)
    answer = bq_client(output)
    final_answer = summarize_with_llm(answer)

    res = {
        "fulfillment_response": {"messages": [
            {"text": {"text": [final_answer]}
                                 }
                                             ]
            }
          }
    return res
# calls code-bison and passes question into LLM, then returns generated SQL


def create_sql(question):

    # initialize vertex API with project and location
    # suggest to make these enviroment variables or configurable
    vertexai.init(project=project, location=location)

    # inject user input (question) into prompt:
    # outlines the schema of the database, this is not specific structure
    # if using a different table, please update schema accordingly
    # inject question into the prompt
    query = "query = {" + question + "}"
    # add rules for LLM to follow.
    fully_scoped_table = project + '.' + dataset + '.' + table
    print(fully_scoped_table)
    rules = '''Rules:
                    1. Please read the question or statement
                        in the section called query.
                    2. Try to map the statement or question in
                        query to columns in the section labeled schema.
                    3. If the query contains a question that requires a
                        column not contained with the section called schema,
                        please state: I am sorry, the table does not contain
                        that information, please limit the questions to orders.
                    4. Turn the question into a BigQuery SQL statement
                        using the table called {} in the dataset called
                        {} within the project called {}.
                    5. Fully scope all table references with {}
            '''.format(table, dataset, project, fully_scoped_table)

    final_prompt = schema + query + rules
    print(final_prompt)
    # set vertex API settings
    parameters = {
        "candidate_count": 1, "max_output_tokens": 1024, "temperature": 0.4}
    model = CodeGenerationModel.from_pretrained("code-bison")
    response = model.predict(final_prompt, **parameters)

    # set model output text to var output
    output = response.text

    # remove general format output by code-bison e.g. '''sql
    output = output[6:]

    # remove format output at the end of response from code-bison e.g. ```
    output = output[:-3]
    print(output)

    # pass output into bq_client function which passes sql into BQ

    # summarized_answer = summarize_with_llm(answer)
    return output
    # pass return value to query into JSON object to Dialogflow

# function that calls BQ client
# accepts query object from query function type: string
# takes result from create_sql


def bq_client(query):
    client = bigquery.Client()
    query_job = client.query(query)
    results = query_job.result()
    json_data = [list(row) for row in results]
    return json_data

# takes output from bq_client and summariezes it in 3 to 4 sentences.


def summarize_with_llm(data):
    model = GenerativeModel("gemini-pro")
    # data = ''.to_list(str(i) for i in data)
    data = str(data)
    summarize_prompt = "Data = {" + data + '''} Please
        summarize this data in a few sentences.
        If there is only a single value,
        just return the single value'''
    responses = model.generate_content(
        summarize_prompt,
        generation_config = {
            "max_output_tokens": 2048,
            "temperature": 0.9,
            "top_p": 1
                            },
        stream=True,
                                      )
    result_of_llm_list = []
    for response in responses:
        result_of_llm_list.append(response.text)
    result_of_llm_string = ''.join(result_of_llm_list)

    return result_of_llm_string
