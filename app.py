import re
from flask import Flask, request, jsonify
import pandas as pd
import requests
from io import BytesIO
import firebase_admin
from firebase_admin import credentials, firestore, storage
from datetime import datetime
import json
import os
from base64 import b64decode
from dotenv import load_dotenv

app = Flask(__name__)

# Initialize Firebase Admin SDK
load_dotenv()

def initialize_firebase():
    """Initialize Firebase app using Base64-decoded service account credentials."""
    try:
        # Retrieve the Base64-encoded credentials from the environment
        encoded_creds = os.getenv("FIREBASE_CREDENTIALS")
        if not encoded_creds:
            raise ValueError("Firebase credentials not found in environment variables.")
        
        # Decode the Base64-encoded credentials
        decoded_creds = b64decode(encoded_creds).decode("utf-8")
        
        # Write the decoded credentials to a temporary file
        temp_cred_path = "temp_firebase_creds.json"
        with open(temp_cred_path, "w") as temp_cred_file:
            temp_cred_file.write(decoded_creds)
        
        # Initialize Firebase
        if not firebase_admin._apps:
            cred = credentials.Certificate(temp_cred_path)
            firebase_admin.initialize_app(cred, {
                "storageBucket": "your-bucket-name.appspot.com"
            })
        
        # Clean up the temporary file
        os.remove(temp_cred_path)
    except Exception as e:
        print(f"Error initializing Firebase: {e}")


# Fetch Excel file from public URL
def fetch_excel_from_url(file_url):
    """
    Fetches an Excel file from a public URL and loads it into a pandas DataFrame.
    
    :param file_url: The public URL of the Excel file.
    :return: A pandas DataFrame or None if the operation fails.
    """
    try:
        # Fetch the file content from the URL
        response = requests.get(file_url)
        response.raise_for_status()  # Raise an HTTPError if the response is not 200
        # Load the file content into a pandas DataFrame
        df = pd.read_excel(BytesIO(response.content))
        return df
    except Exception as e:
        print(f"Error fetching file from URL: {e}")
        return None

# Fetch user references from Firestore
def fetch_user_references(uid):
    """
    Fetches admin references for a given user ID from the users collection in Firestore.
    
    :param uid: The user's unique ID.
    :return: A dictionary containing admin references or an empty dictionary if not found.
    """
    try:
        db = firestore.client()
        user_doc = db.collection("users").document(uid).get()
        if user_doc.exists:
            user_data = user_doc.to_dict()
            return {
                "subDistrictAdminRef": user_data.get("subDistrictAdminRef", None),
                "districtAdminRef": user_data.get("districtAdminRef", None),
                "stateAdminRef": user_data.get("stateAdminRef", None),
            }
        else:
            print(f"No user found with UID: {uid}")
            return {}
    except Exception as e:
        print(f"Error fetching user references: {e}")
        return {}

# Push data to Firestore
def push_data_to_firestore(user_id, df, admin_references, current_month):
    """
    Processes the DataFrame and uploads it to Firestore, including admin references, timestamp, and current month.
    
    :param user_id: The user's unique ID.
    :param df: The pandas DataFrame containing the Excel data.
    :param admin_references: A dictionary containing admin references.
    :param current_month: The current month provided in the request body.
    :return: Document ID of the uploaded data.
    """
    try:
        # Firestore client
        db = firestore.client()
        
        # Extract the fields and values
        fields = df.iloc[3:13, 1].astype(str).tolist()  # Column B, rows 4 to 10 (adjust indexing)
        values = df.iloc[3:13, 7].astype(int).tolist()  # Column H, rows 4 to 10
        
        # Combine fields and values into a dictionary
        data = dict(zip(fields, values))
        
        # Add the user reference
        data['facilityAdminRef'] = f"/users/{user_id}"
        
        # Include admin references in the data
        data.update(admin_references)
        
        # Add current timestamp and month
        data['timestamp'] = datetime.utcnow().isoformat()  # ISO 8601 format
        data['currentMonth'] = current_month
        
        # Push data to Firestore
        doc_ref = db.collection("data").add(data)  # Adds a new document with auto-generated ID
        
        return doc_ref[1].id
    except Exception as e:
        print(f"Error pushing data to Firestore: {e}")
        return None

# Extract file path from the public URL
def extract_file_path(public_url):
    """
    Extracts the file path from a Firebase Storage public URL.
    
    :param public_url: The public URL of the file.
    :return: The file path or None if the extraction fails.
    """
    match = re.search(r'/b/[^/]+/o/(.+?)\?', public_url)
    if match:
        file_path = match.group(1)
        # Decode the URL-encoded path
        return file_path.replace('%2F', '/')
    return None

# Delete file from Firebase Storage
def delete_file(file_path):
    """
    Deletes a file from Firebase Storage.
    
    :param file_path: The file path in Firebase Storage.
    """
    try:
        bucket = storage.bucket()
        blob = bucket.blob(file_path)
        blob.delete()
        print(f"File {file_path} deleted successfully.")
    except Exception as e:
        print(f"Error deleting file: {e}")

# Flask route
@app.route('/upload', methods=['POST'])
def upload_data():
    """
    Endpoint to fetch Excel file from a public URL, upload its contents to Firestore, 
    and delete the file from Firebase Storage.
    
    Request Body (JSON):
    - user_id: The user's unique ID.
    - file_url: The public URL of the Excel file.
    """
    try:
        # Parse request body JSON
        body = request.get_json()
        if not body or 'user_id' not in body or 'file_url' not in body:
            return jsonify({"error": "Missing required parameters: user_id and file_url"}), 400
        
        user_id = body['user_id']
        file_url = body['file_url']
        current_month = body['current_month']
        
        # Initialize Firebase
        initialize_firebase()
        
        # Fetch the Excel file
        df = fetch_excel_from_url(file_url)
        if df is None:
            return jsonify({"error": "Failed to fetch Excel file"}), 500
        
        # Fetch user references
        admin_references = fetch_user_references(user_id)
        
        # Push data to Firestore
        doc_id = push_data_to_firestore(user_id, df, admin_references,current_month)
        if doc_id is None:
            return jsonify({"error": "Failed to upload data to Firestore"}), 500
        
        # Extract file path and delete the file
        file_path_to_delete = extract_file_path(file_url)
        if file_path_to_delete:
            delete_file(file_path_to_delete)
        else:
            return jsonify({"error": "Failed to extract file path from URL"}), 400
        
        return jsonify({"message": "Data uploaded successfully and file deleted", "document_id": doc_id}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Run the server
if __name__ == '__main__':
    app.run(debug=True)
