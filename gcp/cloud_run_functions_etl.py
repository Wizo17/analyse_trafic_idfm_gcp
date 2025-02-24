import os
import subprocess
import json
from flask import Flask, request

app = Flask(__name__)

LOCK_DIR = '/tmp/lock_files/'

# Create folder for lock file
os.makedirs(LOCK_DIR, exist_ok=True)

@app.route("/", methods=["POST"])
def handle_cloud_storage_event():
    """
    Handle Cloud Storage event to trigger Cloud Run function with file lock to avoid concurrency.
    """
    
    # Log the event headers and body for debugging
    print(f"Headers: {request.headers}")
    print(f"Body: {request.data}")
    
    # Type of event : GCS_NOTIFICATION or CloudEvents
    if request.headers.get('ce-type'):
        # CloudEvents format
        event_data = request.get_json()
        print(f"CloudEvent Data: {event_data}")
        try:
            file_name = event_data['name']
        except (KeyError, TypeError):
            print("Bad format: for CloudEvents")
            return "Bad format", 400
    else:
        # GCS_NOTIFICATION old format
        event = request.get_json()
        print(f"GCS Notification Data: {event}")
        try:
            file_name = event.get('name')
        except (KeyError, TypeError):
            print("Bad format: for GCS_NOTIFICATION old")
            return "Bad format", 400

    if not file_name:
        print("No file name found")
        return "No file name found", 400

    print(f"Processing zip file: {file_name}")
    
    # create lock file
    lock_file = os.path.join(LOCK_DIR, f"{file_name}.lock")
    
    # Check if lock file exist
    if os.path.exists(lock_file):
        print(f"Lock file exists for {file_name}, skipping processing.")
        return 'File is already being processed', 200

    with open(lock_file, 'w') as f:
        f.write('locked')

    try:
        result = subprocess.run(
            ['./cloud_run_functions_etl.sh', file_name],
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)
        return 'Success', 200
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")
        return f"Error: {e.stderr}", 500
    except Exception as e:
        print(f"Unhandled Exception: {str(e)}")
        return f"Unhandled Exception: {str(e)}", 500
    finally:
        # Delete lock file
        if os.path.exists(lock_file):
            os.remove(lock_file)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
