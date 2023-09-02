import firebase_admin
import logging
import os
from flask import Flask, request, jsonify, render_template_string, abort
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta, timezone
from fetch_query import get_or_fetch_notams, fetch_notam_by_ids, fetch_notams_with_interpretations
from gpt_notam import fetch_interpret_and_insert_notams, generate_briefing
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_cors import CORS, cross_origin
from flask_caching import Cache
from firebase_admin import credentials, db
from firebase_auth import auth_required

app = Flask(__name__)
CORS(app)

app.config['CACHE_TYPE'] = 'simple'
cache = Cache(app)
DEAFULT_CACHE_TIMEOUT = 900

limiter = Limiter(key_func=get_remote_address)
limiter.init_app(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RTDB_URL = os.getenv('RTDB_URL')
DEFAULT_USER_POINTS = 5

@app.route('/api/notams', methods=['GET'])
@auth_required
@limiter.limit("30 per day")
# @cache.memoize(timeout=DEAFULT_CACHE_TIMEOUT)
def get_notams():
    """
    This endpoint fetches NOTAMs for the given locations and date range.
    It accepts 'locations', 'start_date', and 'end_date' as query parameters.
    It returns a list of NOTAM IDs.
    """
    batch_load_str = request.args.get('batch_load', default='False').lower()
    batch_load = batch_load_str == 'true'
    
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    uid = request.headers.get('uid')

    # Fetch the user's data from Firebase RTDB
    ref = db.reference(f'/users/{uid}')
    user_data = ref.get()

    # Check if user exists
    if not user_data and not batch_load:
        return jsonify({'error': 'User not found'}), 404

    # Check and update points
    if not batch_load:
        first_time_use = user_data.get('first_time_use')
        current_time = datetime.utcnow()
        if first_time_use is None or (current_time - datetime.fromisoformat(first_time_use)) > timedelta(hours=24): 
            ref.update({
                'points': user_data['maximum_points'],
                'first_time_use': current_time.isoformat()
            })
        elif user_data['points'] <= 0:
            return jsonify({'error': 'You have exceeded your request limit'}), 429

    locations = request.args.getlist('locations')
    if not locations:
        return jsonify({'error': 'Missing or empty locations parameter'}), 400

    start_date = request.args.get('start_date') or logger.info(f"Using default start_date {today} for UID {uid}") or today
    end_date = request.args.get('end_date') or logger.info(f"Using default end_date {today} for UID {uid}") or today
    notams, airports_fetched = get_or_fetch_notams(locations, start_date, end_date)
    
    if not batch_load:
        ref.update({
            'points': user_data['points'] - airports_fetched
        })

    if batch_load:
        notam_ids = [notam['notam_id'] for notam in notams]
        fetch_interpret_and_insert_notams(notams, notam_ids)
        return '', 204  # No Content

    return jsonify([notam['notam_id'] for notam in notams])

@app.route('/api/notams/<notams_id>', methods=['GET'])
@auth_required
@limiter.limit("30 per day")
# @cache.memoize(timeout=DEAFULT_CACHE_TIMEOUT)
def get_notam(notams_id):
    """
    This endpoint fetches a specific NOTAM by its ID.
    It also triggers the interpretation of the NOTAM if it hasn't been interpreted yet.
    It returns the NOTAM data as a JSON object or as an HTML table based on the output_type parameter.
    """
    notams = fetch_notam_by_ids(notams_id)
    if notams is None:
        return jsonify({'error': 'NOTAM not found'}), 404
    fetch_interpret_and_insert_notams(notams, notams_id)
    final_notams = fetch_notams_with_interpretations(notams_id)
    return jsonify(final_notams), 200

    
@app.route('/api/briefing/<notams_id>', methods=['GET'])
@auth_required
@limiter.limit("30 per day")
@cache.memoize(timeout=DEAFULT_CACHE_TIMEOUT)
def get_briefing(notams_id):
    """
    This endpoint fetches a specific NOTAM by its ID.
    It also triggers the interpretation of the NOTAM if it hasn't been interpreted yet.
    It returns the NOTAM data as a JSON object.
    """
    # output_type = request.args.get('output_type', 'json')
    role = request.args.get('role', 'flight distpacher')
    if notams_id is None:
        return jsonify({'error': 'notams_id parameter not found'}), 404
    # fetch_interpret_and_insert_notams(notams, notams_id)
    final_notams = generate_briefing(notams_id, role)

    return jsonify(final_notams), 200


@app.route('/api/clear_cache', methods=['POST'])
def clear_cache():
    # Get the UID from the request headers or body
    uid = request.headers.get('uid') or request.json.get('uid')
    if not uid:
        abort(400, "Missing UID")

    # Fetch the user's data from Firebase RTDB
    ref = db.reference(f'/users/{uid}')
    user_data = ref.get()

    # Check if user exists
    if not user_data:
        abort(404, "User not found")

    # Check if the user is an admin
    if user_data.get('role') != "admin":
        abort(403, "Forbidden: User is not an admin")

    # Clear the cache for specific functions
    cache.clear()

    return jsonify({'message': 'Cache cleared successfully'}), 200


# User Get Post

cred = credentials.Certificate('notamify-firebase-adminsdk-j4kwm-0a46563068.json')
firebase_admin.initialize_app(cred, {
    'databaseURL': RTDB_URL 
})

@app.route('/api/save_data', methods=['POST'])
def save_data():
    uid = request.json.get('uid')  # Assuming you're receiving the UID directly in the request
    data = request.json.get('data')
    ref = db.reference(f'/users/{uid}')
    ref.set(data)
    return jsonify({'message': 'Data saved successfully'}), 200

@app.route('/api/get_data/<uid>', methods=['GET'])
def get_data(uid):
    ref = db.reference(f'/users/{uid}')
    data = ref.get()
    return jsonify(data), 200

@app.route('/api/post_signup', methods=['POST'])
def post_signup():
    try:
        # Try to get JSON data from the request
        data = request.get_json(force=True)
    except:
        return jsonify({'error': 'Invalid JSON data'}), 400

    # Get the user's UID and name from the data
    uid = data.get('uid')
    name = data.get('name')

    # Check if UID and name are provided
    if not uid or not name:
        return jsonify({'error': 'UID and name are required'}), 400

    ref = db.reference(f'/users/{uid}')

    # Set the user's data
    ref.set({
        'name': name,
        'maximum_points': DEFAULT_USER_POINTS,
        'points': DEFAULT_USER_POINTS,
        'first_time_use': None
    })


    return jsonify({'message': 'User created successfully'}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
    # app.run(debug=True, host='127.0.0.1', port=8080)