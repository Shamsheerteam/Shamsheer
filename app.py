from flask import Flask, request, jsonify
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore, storage
import requests
from io import BytesIO
import re
from datetime import timezone
import os
from dotenv import load_dotenv

load_dotenv()
encoded_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

decoded_credentials = None
if encoded_credentials:
    import base64
    decoded_credentials = base64.b64decode(encoded_credentials).decode('utf-8')
    with open("firebase_creds.json", "w") as f:
        f.write(decoded_credentials)

cred = credentials.Certificate("firebase_creds.json")
firebase_admin.initialize_app(cred,{'storageBucket': 'kawach-516a2.firebasestorage.app'})
db = firestore.client()        

# Initialize Flask App
app = Flask(__name__)

# Function to extract user ID from the URL
def extract_user_id(file_url):
    try:
        match = re.search(r'/users%2F([^%]+)', file_url)
        if match:
            return match.group(1)
        return None
    except Exception as e:
        print(f"Error extracting user ID: {e}")
        return None

# Function to fetch additional references from the facilityAdminRef document
def fetch_admin_references(facility_admin_ref):
    try:
        # Get the document referred by facilityAdminRef
        facility_admin_doc = facility_admin_ref.get()
        if facility_admin_doc.exists:
            data = facility_admin_doc.to_dict()
            return {
                "subDistrictAdminRef": data.get("subDistrictAdminRef"),
                "districtAdminRef": data.get("districtAdminRef"),
                "stateAdminRef": data.get("stateAdminRef"),
            }
        else:
            print("Facility Admin document does not exist.")
            return {}
    except Exception as e:
        print(f"Error fetching admin references: {e}")
        return {}

# Function to delete a file from Firebase Storage
def delete_file_from_url(file_url):
    try:
        match = re.search(r'/o/(.+)\?alt=media', file_url)
        if not match:
            raise ValueError("Invalid URL format")
        file_path = match.group(1).replace('%2F', '/')
        
        # Access Firebase Storage
        bucket = firebase_admin.storage.bucket()
        blob = bucket.blob(file_path)
        blob.delete()
        return True
    except Exception as e:
        print(f"Error deleting file: {e}")
        return False

@app.route('/upload', methods=['POST'])
def upload_and_delete():
    try:
        # Extract data from the request body
        body = request.json
        url = body.get('url')
        current_month = body.get('currentMonth')

        if not url or not current_month:
            return jsonify({"error": "Missing 'url' or 'currentMonth' in request body"}), 400

        # Download the Excel file from the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Load the Excel file into a Pandas DataFrame
        file_bytes = BytesIO(response.content)
        df = pd.read_excel(file_bytes)

        # Extract the fields and values
        fields = df.iloc[3:13, 1].astype(str).tolist()  # Column B, rows 4 to 10 (adjust indexing for 0-based indexing)
        values = df.iloc[3:13, 7]  # Column H, rows 4 to 10

        if values.isnull().any():
            return jsonify({"error": "One or more required values are empty."}), 400

        if (values < 0).any():
            return jsonify({"error": "One or more required values are negative."}), 400
        values = df.iloc[3:13, 7].astype(int).tolist()   
        # Combine fields and values into a dictionary
        data = dict(zip(fields, values))

        # Extract user ID from the URL
        user_id = extract_user_id(url)
        if user_id:
            # Create a Firestore reference to the user document
            facility_admin_ref = db.collection("users").document(user_id)
            data["facilityAdminRef"] = facility_admin_ref

            # Fetch additional references
            admin_references = fetch_admin_references(facility_admin_ref)
            data.update(admin_references)  # Add subDistrictAdminRef, districtAdminRef, stateAdminRef
        else:
            print("Failed to extract user ID from the URL.")
            data["facilityAdminRef"] = None

        # Add current month and timestamp fields
        data['currentMonth'] = current_month
        data['timestamp'] = firestore.SERVER_TIMESTAMP  # Use Firestore's server timestamp

        # Push data to Firebase
        doc_ref = db.collection("data").add(data)  # Adds a new document with auto-generated ID
        document_id = doc_ref[1].id

        # Deletion as a follow-up task
        deletion_status = delete_file_from_url(url)
        if deletion_status:
            deletion_message = "File successfully deleted."
        else:
            deletion_message = "Failed to delete file."

        return jsonify({
            "message": "Data successfully uploaded",
            "documentId": document_id,
            "deletionStatus": deletion_message
        }), 200

    except Exception as e:
        print(f"Error in /upload endpoint: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))  # Use the PORT from the environment, default to 5000
    app.run(host='0.0.0.0', port=port)
