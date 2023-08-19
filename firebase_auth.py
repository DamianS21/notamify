from flask import Flask, request, jsonify, render_template_string
from firebase_admin import credentials, db, auth
from functools import wraps


def verify_firebase_token(token):
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


def firebase_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'error': 'Authentication token is required'}), 401
        decoded_token = verify_firebase_token(token)
        print(token, decoded_token)
        if decoded_token is None:
            return jsonify({'error': 'Invalid authentication token'}), 401
        return f(*args, **kwargs)
    return decorated_function