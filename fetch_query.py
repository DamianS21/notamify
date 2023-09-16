import pandas as pd
from google.cloud import bigquery
# from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta, timezone
from firebase_admin import db
import hashlib
import os
import re

# load_dotenv()
ICAO_KEY = os.getenv('ICAO_KEY')
NOTAM_API_URL = os.getenv('NOTAM_API_URL') 

def hash_notam_id(input_string):
    return int(hashlib.sha256(input_string.encode()).hexdigest()[:8], 16)

def call_notam_api(locations, api_key=None):
    '''
    Takest list of airports
    Calls ICAO API
    Returns full response
    '''
    if not api_key:
        api_key = ICAO_KEY
    params = {
        'api_key': api_key,
        'format': 'json',
        'criticality': '',
        'locations': locations
    }

    url = NOTAM_API_URL
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch NOTAMs. Status code: {response.status_code}")

def check_existing_notams_keys(notams, table='raw.notams_icao_api'):
    client = bigquery.Client()
    keys = [f"{hash_notam_id(notam['key'])}" for notam in notams]
    query = f"SELECT notam_id FROM notamify.{table} WHERE notam_id IN ({', '.join(keys)})"
    query_job = client.query(query)
    existing_notam_keys = set(row.notam_id for row in query_job.result())  # Extract the notam_id from each row
    return existing_notam_keys


def fetch_existing_notams_from_bq(locations_str, start_date, end_date, current_timestamp, table='raw.notams_icao_api'):
    client = bigquery.Client()
    query = f""" /* fetch_existing_notams_from_bq({locations_str}, {start_date}, {end_date}, {current_timestamp}, {table}) */
    SELECT DISTINCT notam_id, message, startdate, enddate, PERM, EST FROM notamify.{table} WHERE location IN UNNEST(SPLIT({locations_str},',')) AND
    (
    (TIMESTAMP('{start_date}') <= startdate AND TIMESTAMP('{end_date}') >= enddate) OR
    (TIMESTAMP('{start_date}') <= startdate AND startdate <= TIMESTAMP('{end_date}') AND TIMESTAMP('{end_date}') <= enddate) OR
    (startdate <= TIMESTAMP('{start_date}') AND TIMESTAMP('{start_date}') <= enddate AND enddate <= TIMESTAMP('{end_date}')) OR
    (startdate <= TIMESTAMP('{start_date}') AND TIMESTAMP('{start_date}') <= enddate AND TIMESTAMP('{end_date}') <= enddate)
    OR PERM OR EST)
    AND TIMESTAMP_DIFF(TIMESTAMP('{current_timestamp}'), processed_at, MINUTE) <= 15
    """
    query_job = client.query(query)
    return list(query_job.result())

def check_existing_notams_latest_processed_at(notam_keys, table='raw.notams_icao_api'):
    client = bigquery.Client()
    query = f"SELECT processed_at FROM notamify.{table} WHERE notam_id IN UNNEST({list(notam_keys)}) QUALIFY ROW_NUMBER() OVER(PARTITION BY notam_id ORDER BY processed_at ASC) = 1"
    query_job = client.query(query)
    return query_job.result()


def prepare_notam_row(notam):
    processed_at = datetime.now(timezone.utc).isoformat()

    PERM_pattern = re.compile(r'\bC\)\s*PERM\b')
    EST_pattern = re.compile(r'\bC\)\s*\d{6,10}\s*EST\b|\b\d{6}\d{4}-\d{6}\d{4}EST\b')
    EST = bool(EST_pattern.search(notam['all']))

    notam_id_hash = hash_notam_id(notam['key'])

    # Prioritize the date provided in the API output
    startdate = pd.to_datetime(notam.get('startdate'))
    enddate = pd.to_datetime(notam.get('enddate'))

    # If the start date and end date are not provided in the API output, extract them from notam['all']
    PERM_pattern = re.compile(r'\bC\)\s*PERM\b')
    PERM = bool(PERM_pattern.search(notam['all']))
    EST_pattern = re.compile(r'\bC\)\s*\d{6,10}\s*EST\b|\b\d{6}\d{4}-\d{6}\d{4}EST\b')
    EST = bool(EST_pattern.search(notam['all']))

    notam_id_hash = hash_notam_id(notam['key'])

    # Prioritize the date provided in the API output
    startdate = pd.to_datetime(notam.get('startdate'))
    enddate = pd.to_datetime(notam.get('enddate'))

    # If the start date and end date are not provided in the API output, extract them from notam['all']
    if startdate is None or enddate is None: 
        date_pattern = re.compile(r'\b(\d{10})-(\d{10})\b')
        date_match = date_pattern.search(notam['all'])
        if date_match:
            startdate = pd.to_datetime(date_match.group(1), format='%y%m%d%H%M') if startdate is None else startdate
            enddate = pd.to_datetime(date_match.group(2), format='%y%m%d%H%M') if enddate is None else enddate
        else:
            # Handle the edge case with EST
            est_date_pattern = re.compile(r'\b(\d{6})(\d{4})-(\d{6})(\d{4})EST\b')
            est_date_match = est_date_pattern.search(notam['all'])
            if est_date_match:
                startdate = pd.to_datetime(est_date_match.group(1) + est_date_match.group(2), format='%y%m%d%H%M', errors='coerce') if startdate is None else startdate
                enddate = pd.to_datetime(est_date_match.group(3) + est_date_match.group(4), format='%y%m%d%H%M', errors='coerce') if enddate is None else enddate


    return {
        'processed_at': pd.to_datetime(processed_at),
        'notam_id': notam_id_hash,
        'key': notam.get('key'),
        'raw_id': notam.get('id'),
        'location': notam.get('location'),
        'isICAO': notam.get('isICAO'),
        'icao': notam.get('location') if notam.get('isICAO') is True else None,
        'entity': notam.get('entity'),
        'status': notam.get('status'),
        'Qcode': notam.get('Qcode'),
        'Area': notam.get('Area'),
        'SubArea': notam.get('SubArea'),
        'Condition': notam.get('Condition'),
        'Subject': notam.get('Subject'),
        'Modifier': notam.get('Modifier'),
        'message': notam.get('message'),
        'startdate': startdate,
        'enddate': enddate,
        'all': notam.get('all'),
        'Created': pd.to_datetime(notam.get('Created')),
        'type': notam.get('type'),
        'StateCode': notam.get('StateCode'),
        'StateName': notam.get('StateName'),
        'criticality': notam.get('criticality'),
        'PERM': bool(PERM),
        'EST': bool(EST)
    }


def fetch_and_insert_notams(locations):
    '''
    Fetches NOTAMs for the given locations and inserts them into a BigQuery table.

    This function fetches NOTAMs for the specified locations using the NOTAM API. It then checks if the NOTAMs already exist in the BigQuery table. If not, it prepares the NOTAMs for insertion and inserts them into the BigQuery table.

    Args:
        locations (list): A list of airport codes for which to fetch NOTAMs.

    Returns:
        None
    '''
    notams = call_notam_api(locations)
    existing_notams_keys = check_existing_notams_keys(notams)
    rows_to_insert = [prepare_notam_row(notam) for notam in notams if hash_notam_id(notam['key']) not in existing_notams_keys]

    if rows_to_insert:
        client = bigquery.Client()
        table_ref = client.dataset('raw').table('notams_icao_api')
        dataframe = pd.DataFrame(rows_to_insert)
        job = client.load_table_from_dataframe(dataframe, table_ref)
        job.result()

def check_NOTAM(datefrom, dateto, notamfrom, notamto, PERM=False, EST=False):
    '''
    Checks if a NOTAM is active within a given date range.

    This function checks if a NOTAM is active within a specified date range. It considers the NOTAM active if the NOTAM's start and end dates overlap with the specified date range, or if the NOTAM is permanent (PERM) or estimated (EST).

    Args:
        datefrom (datetime): The start date of the date range.
        dateto (datetime): The end date of the date range.
        notamfrom (datetime): The start date of the NOTAM.
        notamto (datetime): The end date of the NOTAM.
        PERM (bool, optional): Indicates if the NOTAM is permanent. Defaults to False.
        EST (bool, optional): Indicates if the NOTAM is estimated. Defaults to False.

    Returns:
        bool: True if the NOTAM is active within the date range, False otherwise.
    '''
    datefrom = datefrom.tz_localize(None) if datefrom.tzinfo else datefrom
    dateto = dateto.tz_localize(None) if dateto.tzinfo else dateto
    notamfrom = notamfrom.tz_localize(None) if notamfrom.tzinfo else notamfrom
    notamto = notamto.tz_localize(None) if notamto.tzinfo else notamto

    if (datefrom <= notamfrom and dateto >= notamto) or (
            datefrom <= notamfrom <= dateto <= notamto) or (
            notamfrom <= datefrom <= notamto <= dateto) or (
            notamfrom <= datefrom <= notamto and dateto <= notamto) or PERM or EST:
        return True
    else:
        return False


def get_or_fetch_notams(locations, start_date, end_date, table='raw.notams_icao_api'):
    # Prepare locations for the query
    locations_str = ', '.join([f"'{loc}'" for loc in locations])


    # Check the last API call time for the given locations
    current_time = datetime.now(timezone.utc)
    should_fetch_locations = {location: False for location in locations}
    for location in locations:
        ref = db.reference(f'/api_call_times/{location}')  # add to add this only if the update was sucessful
        last_call_time = ref.child('last_call_time').get()
        if not last_call_time or (current_time - datetime.fromisoformat(last_call_time)) > timedelta(minutes=15):
            should_fetch_locations[location] = True

            ref.update({'last_call_time': current_time.isoformat()})

    current_processed_at = current_time.isoformat()
    existing_notams = fetch_existing_notams_from_bq(locations_str, start_date, end_date, current_processed_at, table) 
    existing_keys = set(notam.notam_id for notam in existing_notams)

    if not any(should_fetch_locations.values()):
        return existing_notams, 0
    
    all_notams = call_notam_api(locations)
    missing_notams = [notam for notam in all_notams if hash_notam_id(notam['key']) not in existing_keys]

    # Insert missing NOTAMs into BigQuery
    if missing_notams:
        client = bigquery.Client()
        rows_to_insert = [prepare_notam_row(notam) for notam in missing_notams]
        table_ref = client.dataset('raw').table('notams_icao_api')
        dataframe = pd.DataFrame(rows_to_insert)
        job = client.load_table_from_dataframe(dataframe, table_ref)
        job.result()

        # Add the newly inserted NOTAMs to the existing ones
        existing_notams.extend(rows_to_insert)

    # Filter NOTAMs using the check_NOTAM function
    filtered_notam = [notam for notam in existing_notams if check_NOTAM(pd.to_datetime(start_date), pd.to_datetime(end_date), pd.to_datetime(notam['startdate']), pd.to_datetime(notam['enddate']), notam['PERM'], notam['EST'])]
    return filtered_notam, sum(should_fetch_locations.values())


def fetch_notams_with_interpretations(notam_ids):
    client = bigquery.Client()
    query = f'''
    SELECT * FROM `notamify.raw.notams_icao_api` raw
    LEFT JOIN `model.notam_gpt_interpretation` int USING (notam_id)
    WHERE notam_id IN UNNEST({notam_ids})
    QUALIFY ROW_NUMBER() OVER(PARTITION BY notam_id, key ORDER BY raw.processed_at DESC, gpt_model DESC, int.processed_at DESC NULLS LAST) = 1
    '''
    query_job = client.query(query)
    results = list(query_job.result())
    if results:
        return [{field: row[field] for field in row.keys()} for row in results]
    else:
        print(f"NOTAM with ID {notam_ids} not found.")
        return None


def fetch_notam_by_ids(notam_ids):
    client = bigquery.Client()
    query = f"SELECT * FROM notamify.raw.notams_icao_api WHERE notam_id IN UNNEST({notam_ids}) QUALIFY ROW_NUMBER() OVER(PARTITION BY notam_id ORDER BY processed_at DESC) = 1"
    query_job = client.query(query)
    results = list(query_job.result())
    if results:
        return [{field: row[field] for field in row.keys()} for row in results]
    else:
        print(f"NOTAM with ID {notam_ids} not found.")
        return None
    