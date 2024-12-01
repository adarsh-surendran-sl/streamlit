import streamlit as st
import os
import json
import re
import vertexai
from vertexai.generative_models import GenerativeModel, ChatSession
from google.oauth2 import service_account
from vertexai.language_models import TextGenerationModel
from google.cloud import bigquery


def response_from_llm(
    project_id,
    location,
    model_name,
    prompt_level,
    nls_query,
    table_id,
    credentials_json
) -> str:

    # Load credentials from the JSON string
    credentials_dict = json.loads(credentials_json)
    credentials_dict['private_key'] = credentials_dict['private_key'].replace("\\n", "\n")
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)

    # Initialize Vertex AI with the credentials
    vertexai.init(project=project_id, location=location, credentials=credentials)

    # Create a generative model session using gemini
    model = GenerativeModel(model_name)
    chat_session = model.start_chat()


    # Send the NLS prompt and get a response
    prompt_text = f"""
    You are a cricket bot, specialized in creating sql queries for the asked question based on the table schema. 
    Please generate SQL queries only for cricket-related data. We have ball-by-ball data for the matches.
    Now, for the following question:
    '{nls_query}'

    Use the following table with these columns:
        - match_id: A unique identifier for each match. Since the dataset contains ball-by-ball data, every ball is associated with a match ID. However, a different match is indicated only if the match ID is different.
        - match_format: Indicates the type of match (e.g., test, odi, t20, ipl). Note that ipl does not fall under international matches; it is a t20 league.
        - season: The season/year of the match (e.g., 2024/25).
        - start_date: The date the match started,
        - city: The city where the match was played.
        - event: The event associated with the match (e.g., tournament name).
        - venue: The stadium or ground where the match was played.
        - team: The team currently batting (can be the same as batting_team),
        - team2: The second team in the match.
        - toss_winner: The team that won the toss.
        - toss_decision: The decision made by the team winning the toss (e.g., bat, bowl),
        - date: The main match date. For ODIs and T20s, it typically occurs on a single day, with rare exceptions. Test matches are scheduled for 5 days but may finish earlier.
        - date2 to date6: Additional date-related fields, primarily used for Test cricket. date2 may also apply to certain exceptional odi or t20 matches.
        - The columns date_day, date2_day, date3_day, date4_day, date5_day, and date6_day indicate the day of the week(like Sunday, Monday, .....Saturday) corresponding to the respective date columns date, date2, date3, date4, date5, and date6. For any days of the week related queries refer this columns
        - match_number: The match number in the tournament or series.
        - batting_team: The team currently batting.
        - bowling_team: The team currently bowling.
        - gender: Gender category of the match (e.g., male, female).
        - innings: Represents the innings number (1-4), with Test matches having up to 4 innings and T20/ODI matches up to 2. In a Test match, each team can bat or bowl only once per innings. A player's score in one innings is recorded separately from their performance in another innings, unless calculating their overall runs for the match. The same principle applies to bowlers, with their performance tracked separately for each innings.
            Note: 
            - For batters, group by `match_id`, `innings`, and `striker` to tally runs per innings, excluding extras.  
            - For bowlers, group by `match_id`, `innings`, and `bowler` to calculate runs conceded, wickets, and extras per innings.
        - ball: The delivery number in the over (e.g., 0.1, 0.2, ..., 0.6). Each over consists of 6 deliveries unless the bowler bowls extras, such as a wide or no-ball, which may result in additional deliveries.
        - balls_per_over: The number of balls per over in the match. An over typically consists of 6 legal deliveries, unless extras (such as wides or no-balls) are bowled, which may result in additional deliveries.
        - bowler: The name of the player bowling the ball.
        - striker: The name of the batter facing the ball.
        - non_striker: The name of the batter at the non-striking end.
        - runs_off_bat: Runs scored directly off the bat. These should be considered individually for each innings.
        - wides: Runs due to a wide ball.
        - byes: Runs given as byes (ball missed by both bat and keeper).
        - extras: Additional runs given to the batting team (excluding runs off the bat).
        - legbyes: Runs given as leg byes (when the ball hits the batter's body).
        - noballs: Runs due to a no-ball.
        - penalty: Any penalty runs awarded.
        - player_dismissed: Name of the dismissed player, if any.
        - other_player_dismissed: Another player dismissed on the same ball, if applicable.
        - wicket_type: Type of dismissal (e.g., bowled, caught).
        - other_wicket_type: Additional wicket type for unusual dismissals.
        - outcome: The final result or outcome of the match (e.g., win, loss).
        - winner: The team that won the match.
        - winner_innings: The innings number in which the winner was decided.
        - winner_runs: Runs by which the winner won.
        - winner_wickets: Wickets by which the winner won.
        - player_of_match: Name of the player of the match,
        - player_of_match2: Name of a secondary player of the match, if applicable.
        - match_referee: The referee overseeing the match.
        - method: The method used to resolve the match (e.g., D/L method).
        - umpire: The on-field umpire.
        - umpire2: The second on-field umpire.
        - Umpire3: The third on-field umpire. 
        - reserve_umpire: Name of the reserve umpire,
        - reserve_umpire2: Name of the second reserve umpire, typically present only in Test cricket matches.
        - tv_umpire: Name of the TV umpire.
        - tv_umpire2: Name of the second TV umpire, typically present only in Test cricket matches.

    This is the table ID: {table_id}.

    Key points: 
        - If the striker's name is not in the player_dismissed column, it means that the striker was not out in that particular innings.
        - If a player scores a fifty, it means the score should be between 50 and less than 100. If the player scores 100 runs or more, it will be counted in the centuries list, not in the fifties list.

    """

    # Send the message to the chat session and get the response
    llm_response = get_chat_response(chat_session, prompt_text)

    return llm_response


def re_response_from_llm(
    project_id,
    location,
    model_name,
    prompt_level,
    nls_query,
    table_id,
    credentials_json,
    sql_query,
    error_message
) -> str:

    # Load credentials from the JSON string
    credentials_dict = json.loads(credentials_json)
    credentials_dict['private_key'] = credentials_dict['private_key'].replace("\\n", "\n")
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)

    # Initialize Vertex AI with the credentials
    vertexai.init(project=project_id, location=location, credentials=credentials)

    # Create a generative model session using gemini
    model = GenerativeModel(model_name)
    chat_session = model.start_chat()
    print("preparing second prompt")

    # Send the NLS prompt and get a response
    prompt_text = f"""

    I have got error from your previous query. Previous query was '{sql_query}'
    error is '{error_message}'.

    please regenerate the proper query. 

    """

    #print("second time prompt_text :", prompt_text)
    # Send the message to the chat session and get the response
    re_llm_response = get_chat_response(chat_session, prompt_text)
    #print("re_llm_response :", re_llm_response)

    return re_llm_response


def output_formatter_prompt(
    project_id,
    location,
    model_name,
    nls_query,
    final_output,
    credentials_json,
) -> str:

    # Load credentials from the JSON string
    credentials_dict = json.loads(credentials_json)
    credentials_dict['private_key'] = credentials_dict['private_key'].replace("\\n", "\n")
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)

    # Initialize Vertex AI with the credentials
    vertexai.init(project=project_id, location=location, credentials=credentials)

    # Create a generative model session using gemini
    model = GenerativeModel(model_name)
    chat_session = model.start_chat()

    # Send the NLS prompt and get a response
    prompt_text = f"""
    Your task is to create a sentence from the given query and output.
    user query is '{nls_query}'
    output is '{final_output}'

    please genrate proper sentence from the provided output only.
    """

    #print("second time prompt_text :", prompt_text)
    # Send the message to the chat session and get the response
    formatted_answer = get_chat_response(chat_session, prompt_text)
    #print("query_type_llm_response :", query_type_llm_response)

    return formatted_answer

def find_realted_question_or_not(
    project_id,
    location,
    model_name,
    nls_query,
    credentials_json,
) -> str:

    # Load credentials from the JSON string
    credentials_dict = json.loads(credentials_json)
    credentials_dict['private_key'] = credentials_dict['private_key'].replace("\\n", "\n")
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)

    # Initialize Vertex AI with the credentials
    vertexai.init(project=project_id, location=location, credentials=credentials)

    # Create a generative model session using gemini
    model = GenerativeModel(model_name)
    chat_session = model.start_chat()

    # Send the NLS prompt and get a response
    prompt_text = f"""
    I have cricket data from the past 20 years, covering formats like Test, ODI, and T20 International matches.

    query_is: '{nls_query}'

    Your task:
    - Determine if the given query is related to cricket.
    - If the query is not related to cricket, return general
    - If the query is related to cricket, return cricket'

    Output format: should be dictionary with the key as query_type value should be either general or cricket.
    """

    #print("second time prompt_text :", prompt_text)
    # Send the message to the chat session and get the response
    query_type = get_chat_response(chat_session, prompt_text)
    #print("query_type_llm_response :", query_type_llm_response)

    return query_type


def get_chat_response(chat: ChatSession, prompt: str) -> str:
    text_response = []
    responses = chat.send_message(prompt, stream=True)
    for chunk in responses:
        text_response.append(chunk.text)
    return "".join(text_response)

def clean_and_extract_query(llm_response: str) -> str:
    """Extract and clean the SQL query from the LLM response."""
    # Use regex to extract the content inside triple backticks
    match = re.search(r"```(?:sql)?\n?(.*?)```", llm_response, re.DOTALL)
    query = match.group(1) if match else llm_response

    # Strip any extra spaces or newlines
    return query.strip()

def output_formatter(prompt, final_output):

    prompt_level = 1

    # Call the function to generate the SQL query
    llm_res = output_formatter_prompt(
    project_id,
    location,
    model_name,
    nls_query,
    final_output,
    credentials_json,
    )

    return llm_res

def query_type_finder(prompt):

    prompt_level = 1

    # Call the function to generate the SQL query
    llm_res = find_realted_question_or_not(
    project_id,
    location,
    model_name,
    nls_query,
    credentials_json,
    )

    return llm_res

def nls_to_sql(prompt):

    prompt_level = 1

    # Call the function to generate the SQL query
    llm_res = response_from_llm(
        project_id,
        location,
        model_name,
        prompt_level,
        nls_query,
        table_id,
        credentials_json
    )

    # Clean and extract the SQL query from the LLM response
    query = clean_and_extract_query(llm_res)

    return query


def nls_to_sql_again(prompt, sql_query, error_message):

    prompt_level = 1

    # Call the function to generate the SQL query
    llm_res = re_response_from_llm(
        project_id,
        location,
        model_name,
        prompt_level,
        nls_query,
        table_id,
        credentials_json,
        sql_query,
        error_message
    )

    # Clean and extract the SQL query from the LLM response
    query = clean_and_extract_query(llm_res)

    return query


def fetch_data_from_bigquery(sql_query, project_id, credentials_json):
    try:
        status = "success"
        error_message = ""
        #print("going to execute :", sql_query)
        # Load credentials from the JSON string
        credentials_dict = json.loads(credentials_json)
        credentials_dict['private_key'] = credentials_dict['private_key'].replace("\\n", "\n")
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)

        # Initialize BigQuery client
        client = bigquery.Client(project=project_id, credentials=credentials)

        print(f"Executing SQL Query:\n{sql_query}\n")

        # Run the query
        query_job = client.query(sql_query)

        # Fetch results
        results = query_job.result()
        data = [dict(row) for row in results]

        #print(f"Query Results:\n{data}\n")
        return data, status, error_message

    except Exception as e:
        status = "failure"
        error_message = str(e)
        #print(f"Error occurred: {error_message}")
        return {"error": error_message}, status, error_message


def get_output(nls_query):
    try:
        # Simulating the query_type_finder function's output
        query_type = query_type_finder(nls_query)  # Replace this with actual function

        # Try to extract JSON part using regex
        try:
            match = re.search(r'\{.*\}', query_type)
            if match:
                query_type = match.group(0)
            else:
                print("No valid JSON found in query_type. Proceeding as cricket-related query.")
                query_type = None  # Default to None if no valid JSON
        except Exception as e:
            print(f"Error during regex extraction: {e}")
            query_type = None  # Default to None if error occurs

        # Parse the JSON part
        try:
            if query_type:
                query_type = json.loads(query_type)
            else:
                query_type = {}  # Default to empty dict if parsing fails
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            query_type = {}  # Default to empty dict if error occurs

        # Check if the query is cricket-related
        try:
            if query_type.get("query_type") == "cricket":
                print("Cricket-related query detected.")
            elif query_type.get("query_type") == "general":
                print("General query detected.")
                return "Please rephrase your question"
            else:
                print("Unknown query type. Proceeding as cricket-related query.")
        except Exception as e:
            print(f"Error checking query_type: {e}. Proceeding as cricket-related query.")

        # Proceed to SQL conversion
        sql_query = nls_to_sql(nls_query)  # Replace with actual function
        #print("SQL Query:", sql_query)

        # Fetch data from BigQuery (example logic)
        try:
            data, status, error_message = fetch_data_from_bigquery(sql_query, project_id, credentials_json)  # Replace with actual implementation
            if status == "success":
                answer = output_formatter(nls_query, data)  # Replace with actual function
                return answer
            elif status == "failure":
                print("Retrying query...")
                latest_query = nls_to_sql_again(nls_query, sql_query, error_message)  # Replace with actual function
                data, status, error_message = fetch_data_from_bigquery(latest_query, project_id, credentials_json)
                if status == "success":
                    answer = output_formatter(nls_query, data)
                    return answer
                else:
                    return "Please rephrase your question"
        except Exception as e:
            print(f"Error fetching data from BigQuery: {e}")
            return "Error fetching data. Please try again later."

    except Exception as e:
        print(f"Unexpected error in get_output function: {e}")
        return "An unexpected error occurred. Please try again later."



# Your service account credentials (credentials JSON string)
credentials = {
    "type": "service_account",
    "project_id": "pristine-dahlia-442517-m8",
    "private_key_id": "16de14b99226b91c67abf09be93eb4e2b5deb211",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCICYaxzTFMJLsa\nNhd2nMO0XdOM7MiSQ+ZsIwd0B8xm5steDFoFKfcVkJn2ROq56/lbR2ObWmS40IzN\nItFZRvvsUON2vKxAH2LVGeWk/tAHwUa9DTwkRxurHsM51rVp11vmvd5StEJQ9Knr\nZxPnVQfdCNeXplkjP8E/Jv8HiO9EbuN/Cqf3uC1irMNkMm1tR3QT6bMJBQfhl975\nXl6xDEPPgzI/UV4Dlxnr70+6j0mqY/Kf2FVRquHVDLfvP7R0VAPCv5gjvxFrZ2Qb\nMHj9uceWHzrAdpOPB8r+NWU47Yp7HgKQOZrtvu6VnP4OafzBgG05E3xgFIFcm41Y\nP7eW/HbDAgMBAAECggEADNw1rw5SWDvqsi4l5rEoMR9jleF2jpk7qcJf/ICWOq4b\n0J7DTdSrRo8edvEJ7ZyYvJ/Rk0im1+/jN6tQdiunOi5lan6onAE7kyC5HHF4Uhtb\n6BhdtKJCQ9mLJCcfjEtvJS+S2mevEz8l5xqd/5MCxMsGso/nwlTPHHy/xEMWN3ZO\n4lYNL4qh+oXDF5TFs6q51gHN9t9fIaXTreeVLNEPGASfDg6ecYAFdUQ96PRDlrKZ\nms7zrcrRBYIUv5WU1lD/akAXF+51h8oW34ywBGgCbBA+PDJv92p57e9iyk6kaGVo\naiCLDawiSUABuloY5X2E+sQqCgzypqvA/8GFEb8CbQKBgQC96E+hxtWK/BFj3sH1\nQtoq7TKNuFuzFS5ERGUMvE+XJC0xalDRxqjrjBW5vmlwgkri4XV92NoWHhHo70C6\nwdnYQ/Dx8F5qCl+7zUQNNgZP0Ow86ixcU7fHtTOAU1N+Z8KvbiiqbyHPzzgPwzVJ\n9CyIjrM6FPj58Xgf723d+eh0bwKBgQC3Ya2NiCQAlYQleQHtBaehQdYd1CuNUulv\nlGHLliUoFwH+/GTVChudVAYpx5TKolYUQFtXsw6JN4sr0blqduu6rj4jTAsp5k26\n30PksznJX/pur3ESHIgCHHjDa2EJcz+GJDy7Q96C5h28ofHFGnSewqqaLD3mLoaB\np++LwVAU7QKBgGmbb/U6GnSQ2qbcMKZQh/yJLazMEgqTw8fo9PzRF72oO42YXttZ\ni4R5wXcqoX4UpspmsKzMU/Mqw7Dyo993f8qZdbtFfWug2z4X1zk1iusF9umlHAg3\n8iKX7Mz3CZ5o20Ytj2XAKWBkDvwNBWxb4gwKtzachRyRoMi5oDJqEE/3AoGAUb9i\nXahl3DjKuuWxnRabPoCZ0ZxGee6PD2WHHvlkwPVYt3GOBYZG/yB4yuEkjBxH4Sk7\nTuIMRdET4knQrQwsU4DfRF7ezyPSXM6wdmALD6EQg40EC73aR46nqv3QOSU+GS9I\nP3am15V7hh5vJ51+hVNkN+wm/9iXr27Mk3FuoJUCgYA21hfFacrgPJNEwtxIA5mU\n9dag0+3ctEdDvfLTYPmNVc4l/zjHQiO4PHwnU80Ny65uI1Ul4044lwaS4rtFzji4\nXpW/gsH0M0j47rlq8AsFGtepKFtdj56IN9sAzGBu+8G8seX5nejmSk0C48l/MyoN\nv49zrVhWMRFLNVXcsqpasw==\n-----END PRIVATE KEY-----\n",
    "client_email": "adarsh@pristine-dahlia-442517-m8.iam.gserviceaccount.com",
    "client_id": "108910322993158451625",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/adarsh%40pristine-dahlia-442517-m8.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}


# Encode the credentials to JSON string
credentials_json = json.dumps(credentials, indent=4)

project_id = "pristine-dahlia-442517-m8"
location = "us-central1"
table_id = "pristine-dahlia-442517-m8.hackathon.cricket_hackathon_data"
model_name = "gemini-1.5-flash-002"

# Streamlit app layout
st.title("Cricket Query Analyzer")

# Input box for natural language query
nls_query = st.text_input("Enter your cricket-related query:")

# Button to trigger the function
if st.button("Submit"):
    if nls_query.strip():
        # Call your function
        try:
            result = get_output(nls_query)
            st.success("Query Processed Successfully!")
            st.write("Result:", result)
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.warning("Please enter a valid query!")