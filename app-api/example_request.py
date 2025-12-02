import requests
import sys
import json

BASE_URL = "https://app-api-2555497750722384.4.azure.databricksapps.com"
PROCESS_ID = "my-project2"
PROCESS_NAME = "My Project2"

# Get bearer token from command line argument
if len(sys.argv) < 2:
    print("Error: Bearer token required")
    print("Usage: python example_request.py <bearer_token>")
    sys.exit(1)

bearer_token = sys.argv[1]
headers = {"Authorization": f"Bearer {bearer_token}"}

# Create a process
print("Creating process...")
response = requests.post(
    f"{BASE_URL}/process",
    json={
        "process_id": PROCESS_ID,
        "process_name": PROCESS_NAME,
        "milestones": ["Start", "Middle", "End"]
    },
    headers=headers
)
if 'error' in response.json():
    print("Error creating process:", response.json()['error'])
    sys.exit(1)
print("✓ Process created successfully")

# Get process status
print("\nGetting status...")
response = requests.get(f"{BASE_URL}/process/{PROCESS_ID}", headers=headers)
process_data = response.json()
completed = process_data.get('completed_milestones', 0)
total = process_data.get('total_milestones', 0)

print(f"Completed milestones: {completed}/{total}")

if completed == 0:
    status = "Pending"
elif completed < total:
    status = "Started"
else:
    status = "Completed"

print(f"Status: {status}")
