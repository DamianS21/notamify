import os
import openai
import pandas as pd
from google.cloud import bigquery
# from dotenv import load_dotenv
from datetime import datetime
from fetch_query import prepare_notam_row, check_existing_notams_keys, fetch_notam_by_ids

# load_dotenv()
GPT_MODEL = "gpt-4-0613"
GPT_MODEL_BRIEFING = "gpt-4" # "gpt-3.5-turbo-16k"  # "gpt-3.5-turbo"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
os.environ['OPENAI_API_KEY'] = OPENAI_API_KEY

function_descriptions = [
    {
        "name": "extract_info_from_notam",
        "description": "extract and interprate key info from an NOTAM (Notice to Airman), such as short description, category, impacted role (ATC,Flight Dispatcher, etc).",
        "parameters": {
            "type": "object",
            "properties": {
                "notamShortDescription": {
                    "type": "string",
                    "description": "The most important informations of the NOTAM summarized to one or two setences"
                },                                        
                "notamDescription": {
                    "type": "string",
                    "description": "Interpretation of the NOTAM"
                },
                "category": {
                    "type": "string",
                    "description": "Try to categorise this NOTAM into categories like those: 1. Opening Hours; 2. Airport maintenance; 3. Airspace closure; 4. Obstacle; etc."
                },
                "impactedRole":{
                    "type": "string",
                    "description": "Try to categorise which role should be informed about this NOTAM. Return value as string for roles after comma. (Pilot, Flight Dispatcher, ATC)."
                },
            },
            "required": ["notamShortDescription", "notamDescription", "category", "impactedRole"]
        }
    }
]



def interpret_notam_with_gpt(notam_message):
    # Call the OpenAI Chat Completions API to interpret the NOTAM
    prompt = f"Please extract key information from this NOTAM: {notam_message} "
    message = [{"role": "user", "content": prompt}] 
    response = openai.ChatCompletion.create(
        model=GPT_MODEL,
        api_key=OPENAI_API_KEY,
        messages=message,
        functions = function_descriptions,
        function_call="auto"
    )
    arguments = response.choices[0]["message"]["function_call"]["arguments"]
    short_interpretation = eval(arguments).get("notamShortDescription")
    interpretation= eval(arguments).get("notamDescription")
    category= eval(arguments).get("category")
    roles= eval(arguments).get("impactedRole")

    return short_interpretation, interpretation, category, roles


def prepare_gpt_interpretation_row(notam, short_interpretation, interpretation, category, roles):
    return {
        'notam_id': notam['notam_id'],
        'notam_content': notam['message'] if notam['message'] else notam['all'],
        'gpt_model': GPT_MODEL,
        'gpt_short_interpretation': short_interpretation,
        'gpt_interpretation': interpretation,
        'gpt_category': category,
        'gpt_interpretation_role': roles,
        'processed_at': datetime.now().isoformat()
    }

def insert_gpt_interpretation_into_bigquery(rows_to_insert):
    client = bigquery.Client()
    table_ref = client.dataset('model').table('notam_gpt_interpretation')
    dataframe = pd.DataFrame(rows_to_insert)
    job = client.load_table_from_dataframe(dataframe, table_ref)
    job.result()

def check_interpretation_exists(notam_id, model=GPT_MODEL):
    client = bigquery.Client()
    query = f"""
    SELECT notam_id FROM UNNEST({notam_id}) notam_id
    EXCEPT DISTINCT
    SELECT notam_id FROM notamify.model.notam_gpt_interpretation WHERE notam_id IN UNNEST({notam_id}) AND gpt_model = '{model}'
    """
    query_job = client.query(query)
    notam_ids = [row['notam_id'] for row in query_job.result()]
    return notam_ids

def fetch_interpret_and_insert_notams(notams, notam_ids):
    # Fetch NOTAM by ID from BigQuery
    if notams is None:
        print(f"NOTAMs not found. Skipping interpretation.")
        return

    notam_ids_to_interpret = check_interpretation_exists(notam_ids) 
    print(notam_ids_to_interpret)
    if notam_ids_to_interpret:
        for notam in notams:
            if notam['notam_id'] in notam_ids_to_interpret:
                message = notam['message'] if notam['message'] else notam['all'] 
                short_interpretation, interpretation, category, roles = interpret_notam_with_gpt(message)
                interpretation_row = prepare_gpt_interpretation_row(notam, short_interpretation, interpretation, category, roles)
                insert_gpt_interpretation_into_bigquery([interpretation_row])
    
    return None


# Briefing

def fetch_interpretations_from_bigquery(notam_ids):
    client = bigquery.Client()
    query = f"""
    SELECT icao, gpt_short_interpretation, gpt_category, gpt_interpretation_role 
    FROM notamify.model.notam_gpt_interpretation
    INNER JOIN
    (SELECT notam_id, icao FROM notamify.raw.notams_icao_api QUALIFY ROW_NUMBER() OVER(PARTITION BY notam_id ORDER BY processed_at DESC) = 1) USING (notam_id)
    WHERE notam_id IN UNNEST ({notam_ids})
    ORDER BY icao, gpt_interpretation_role
    """
    query_job = client.query(query)
    return list(query_job.result())

def generate_briefing(notam_ids, role):
    # Fetch the interpretations from BigQuery
    interpretations = fetch_interpretations_from_bigquery(notam_ids)

    # Format the interpretations into a briefing
    notams = ";\n".join([f"Airport: {interp['icao']}. NOTAM: {interp['gpt_short_interpretation']}\n" for interp in interpretations])

    # Use the GPT model to generate a summary of the briefing
    prompt = f"You are tasked with providing a briefing for a {role} based solely on the provided NOTAMs. Your goal is to extract and present only the information that is directly relevant to the responsibilities of a {role}. Please prioritize the most critical information and keep the briefing concise and to the point. Do not include any information that is not directly related to the role of a {role}. Begin the briefing with the phrase 'Here is your briefing' and format the briefing in Markdown. Use the following NOTAMs as your source of information:\n{notams}."
    message = [{"role": "user", "content": prompt}]
    response = openai.ChatCompletion.create(
        model=GPT_MODEL_BRIEFING,
        api_key=OPENAI_API_KEY,
        messages=message
    )

    # Extract the summary from the response
    summary = response.choices[0].message.content.strip()

    return summary
