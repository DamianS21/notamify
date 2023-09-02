import os
from flask import Flask, request, jsonify, render_template_string
from firebase_admin import credentials, db, auth
from functools import wraps

INTERNAL_AUTH_KEY = os.getenv("INTERNAL_AUTH_KEY")


def _is_internal_request():
    return request.headers.get('Internal-Auth-Token') == INTERNAL_AUTH_KEY


def _verify_firebase_token(token):
    """
    Verifies the Firebase token.
    Args:
    - token (str): The Firebase token.
    
    Returns:
    - dict: Decoded token if valid, None otherwise.
    """
    try:
        # Verify the token
        decoded_token = auth.verify_id_token(token)
        return decoded_token
    except Exception as e:
        print(f"Token verification failed: {e}")
        return None


def auth_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        batch_load_str = request.args.get('batch_load', default='False').lower()
        batch_load = batch_load_str == 'true'
        
        if _is_internal_request():
            if batch_load is True:
                return f(*args, **kwargs)
            else:
                return jsonify({'error': 'batch_load flag is not set for internal request'}), 400
      
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'error': 'Authentication token is required'}), 401
        decoded_token = _verify_firebase_token(token)

        if decoded_token is None:
            return jsonify({'error': 'Invalid authentication token'}), 401

        if batch_load is True:
            return jsonify({'error': 'batch_load flag is only available for internal requests'}), 400

        return f(*args, **kwargs)
    return decorated_function