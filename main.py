from openai import OpenAI
import os
import openai
import sqlite3
import json
import time
import re

client = OpenAI()


openai.api_key = os.getenv('OPENAI_API_KEY')


DATA_MASTER_ID = os.getenv('DATA_MASTER_ID')

def show_json(obj):
    # Parse the JSON string
    parsed_json = json.loads(obj.model_dump_json())

    # Convert the JSON object to a pretty-printed string
    pretty_json = json.dumps(parsed_json, indent=4)

    # Print the pretty-printed JSON
    print(pretty_json)

# Pretty printing helper
# parse model SQL response here as well

model_sql_query =[]

def extract_sql_statements(text):
    """
    Extracts SQL statements from a string and removes newline characters.
    The SQL statements are expected to be enclosed within triple backticks followed by 'sql'.
    Example:
    ```sql
    SELECT * FROM table;
    ```

    Args:
    text (str): The string containing the SQL statements.

    Returns:
    list: A list of SQL statements found in the text, with newline characters removed.
    """
    pattern = r"```sql\s+(.*?)\s+```"
    matches = re.findall(pattern, text, re.DOTALL)
    cleaned_matches = [match.replace('\n', ' ') for match in matches]
    return cleaned_matches

def pretty_print(messages):
    print("# Messages")
    for m in messages:
        print(f"{m.role}: {m.content[0].text.value}")
        if m.role == "assistant":
            # print(type(m.content[0].text.value)) debugging
            to_list = extract_sql_statements(m.content[0].text.value)
            # print(to_list) debugging
            model_sql_query.extend(to_list)
    print()



def submit_message(assistant_id, thread, user_message):
    client.beta.threads.messages.create(
        thread_id=thread.id, role="user", content=user_message
    )
    return client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=assistant_id,
    )


def get_response(thread):
    return client.beta.threads.messages.list(thread_id=thread.id, order="asc")

def create_thread_and_run(user_input):
    thread = client.beta.threads.create()
    run = submit_message(DATA_MASTER_ID, thread, user_input)
    return thread, run

# Waiting in a loop
def wait_on_run(run, thread):
    while run.status == "queued" or run.status == "in_progress":
        run = client.beta.threads.runs.retrieve(
            thread_id=thread.id,
            run_id=run.id,
        )
        time.sleep(0.5)
    return run




def sqlite_quizza(query):

    for i in query:
        i = str(i)
        # print(f"this is SQL data type being passed: {type(i)}") debugging
        conn = sqlite3.connect('amazon_reviews.sqlite')
        cursor = conn.cursor()

        cursor.execute(i)

        # Check if the query is a SELECT query
        if i.strip().upper().startswith('SELECT'):
            # Fetch and print all the results
            results = cursor.fetchall()
            for row in results:
                print(row)
        else:
            # For non-SELECT queries, commit the changes
            conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()

thread = client.beta.threads.create()



def get_multiline_input(prompt):
    print(prompt)
    lines = []
    while True:
        line = input()
        if not line.strip():
            break
        lines.append(line)
    return '\n'.join(lines)


exit_code = 0
continue_code = 0

while exit_code != 1:
    if continue_code == 0:
        user_in = get_multiline_input('''Chat to Data Master: ''')
        thread1, run1 = create_thread_and_run(user_in)
        run1 = wait_on_run(run1, thread1)
        pretty_print(get_response(thread1))
        continue_code = 1
        if len(model_sql_query) >= 1:
            try:
                sqlite_quizza(model_sql_query)
                model_sql_query.clear()
            except Exception as e:
                print(e)
                print("SQL couldn't be preformed, check in with the alchemist")
    elif continue_code == 1:
        user_in = get_multiline_input('''Chat to Data Master: ''')
        run1 = submit_message(DATA_MASTER_ID, thread1, user_in)
        run1 = wait_on_run(run1, thread1)
        # run_steps = client.beta.threads.runs.steps.list(
        #     thread_id=thread1.id, run_id=run1.id, order="asc"
        # )
        # for step in run_steps.data:
        #     step_details = step.step_details
        #     print(json.dumps(show_json(step_details), indent=4)) debugging
        pretty_print(get_response(thread1))
        if len(model_sql_query) >= 1:
            try:
                sqlite_quizza(model_sql_query)
                model_sql_query.clear()
            except Exception as e:
                print(e)
                print("SQL couldn't be preformed, check in with the alchemist")

