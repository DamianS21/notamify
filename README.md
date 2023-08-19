# Notamify Backend API 

Notamify Backend API is a web service that provides real-time NOTAM (Notice to Airmen) information and user handling functionalities. The API is built using Flask and integrates with various services such as BigQuery, Firebase Auth, Firebase Realtime Database, OpenAI GPT-4 API, and ICAO NOTAM API.


## Technologies Used

- **ICAO NOTAM API**: The application fetches real-time NOTAM information from the ICAO NOTAM API.
- **Firebase Auth**: The application uses Firebase Authentication for user authentication and authorization.
- **Firebase Realtime Database**: The application uses Firebase Realtime Database for storing and retrieving user data.
- **BigQuery**: The application uses BigQuery for storing NOTAMs and GPT Interpretations.
- **OpenAI GPT-4 API**: The application uses the OpenAI GPT-4 API for natural language processing tasks.

## Features

- **Real-time NOTAM Information**: The application provides real-time NOTAM information to users.
- **User Authentication**: The application supports user authentication using Firebase Authentication.
- **Natural Language Processing**: The application uses the OpenAI GPT-4 API for natural language processing tasks.

## gpt_notam.py

This file contains functions to interpret NOTAMs (Notice to Airmen) using the OpenAI GPT model and insert the interpretations into BigQuery.

### Functions

- `interpret_notam_with_gpt(notam_message)`: Calls the OpenAI Chat Completions API to interpret the NOTAM and extract key information such as short description, category, and impacted role (ATC, Flight Dispatcher, etc).

- `prepare_gpt_interpretation_row(notam, short_interpretation, interpretation, category, roles)`: Prepares a row for insertion into BigQuery with the NOTAM ID, content, GPT model, short interpretation, interpretation, category, roles, and processed timestamp.

- `insert_gpt_interpretation_into_bigquery(rows_to_insert)`: Inserts the GPT interpretation rows into the BigQuery table `notam_gpt_interpretation`.

- `check_interpretation_exists(notam_id, model=GPT_MODEL)`: Checks if the interpretation for the given NOTAM ID and model already exists in BigQuery.

- `fetch_interpret_and_insert_notams(notams, notam_ids)`: Fetches NOTAMs by ID from BigQuery, interprets them with GPT, and inserts the interpretations into BigQuery.

- `fetch_interpretations_from_bigquery(notam_ids)`: Fetches the interpretations from BigQuery for the given NOTAM IDs.

- `generate_briefing(notam_ids, role)`: Generates a briefing for the given role based on the NOTAM interpretations.

## api.py

This file contains the Flask API endpoints for fetching NOTAMs, interpreting NOTAMs, generating briefings, and managing user data.

### Endpoints

- `/api/notams`: Fetches NOTAMs for the given locations and date range. Returns a list of NOTAM IDs.

- `/api/notams/<notams_id>`: Fetches a specific NOTAM by its ID, triggers the interpretation if it hasn't been interpreted yet, and returns the NOTAM data.

- `/api/briefing/<notams_id>`: Fetches a specific NOTAM by its ID, triggers the interpretation if it hasn't been interpreted yet, and returns the NOTAM data as a briefing.

- `/api/clear_cache`: Clears the cache for specific functions.

- `/api/save_data`: Saves user data to Firebase Realtime Database.

- `/api/get_data/<uid>`: Retrieves user data from Firebase Realtime Database.

- `/api/post_signup`: Creates a new user with the provided UID and name.

## fetch_query.py

This file contains functions to fetch NOTAMs from the ICAO API, insert them into BigQuery, and fetch existing NOTAMs from BigQuery.

### Functions

- `hash_notam_id(input_string)`: Hashes the NOTAM ID.

- `call_notam_api(locations, api_key=None)`: Calls the ICAO API to fetch NOTAMs for the given locations.

- `check_existing_notams_keys(notams, table='raw.notams_icao_api')`: Checks if the NOTAMs already exist in BigQuery.

- `fetch_existing_notams_from_bq(locations_str, start_date, end_date, table='raw.notams_icao_api')`: Fetches existing NOTAMs from BigQuery for the given locations and date range.

- `prepare_notam_row(notam)`: Prepares a row for insertion into BigQuery with the NOTAM data.

- `fetch_and_insert_notams(locations)`: Fetches NOTAMs from the ICAO API, checks if they already exist in BigQuery, and inserts the missing NOTAMs into BigQuery.

- `check_NOTAM(datefrom, dateto, notamfrom, notamto, PERM=False, EST=False)`: Checks if the NOTAM is valid for the given date range.

- `get_or_fetch_notams(locations, start_date, end_date, table='raw.notams_icao_api')`: Fetches existing NOTAMs from BigQuery or fetches and inserts them from the ICAO API.

- `fetch_notams_with_interpretations(notam_ids)`: Fetches NOTAMs with interpretations from BigQuery.

- `fetch_notam_by_ids(notam_ids)`: Fetches NOTAMs by ID from BigQuery.


## Live

Project is live on www.notamify.com
Live website uses FrontEnd No-Code tools like Webflow.com and Wized.com sending request to the API.
