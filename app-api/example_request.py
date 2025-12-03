import requests
import sys
import json
import time

BASE_URL = "https://app-api-2555497750722384.4.azure.databricksapps.com"
PROCESS_ID = "my-project2"
PROCESS_NAME = "My Project2"

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

token = w.tokens.create(comment=f"sdk-{time.time_ns()}", lifetime_seconds=300)
print("Generated temporary token:", token.token_value)

# Get bearer token from command line argument
if len(sys.argv) < 2:
    print("Error: Bearer token required")
    print("Usage: python example_request.py <bearer_token>")
    sys.exit(1)

bearer_token = sys.argv[1]
headers = {"Authorization": f"Bearer {bearer_token}"}
# Add JSON headers to request
headers.update({
    "Accept": "application/json",
    "Content-Type": "application/json",
})

# Create a process (send payload matching app.py: id, name, milestones)
payload = {
    "id": PROCESS_ID,
    "name": PROCESS_NAME,
    "milestones": ["Start", "Middle", "End"],
}
url = f"{BASE_URL}/process"

print("Creating process...")
print("POST", url)
print("Payload:", payload)
print("Headers: Authorization: <redacted>, Accept: application/json, Content-Type: application/json")

# Show an equivalent redacted curl command for debugging
curl_cmd = (
    f"curl -i -X POST '{url}' -H 'Authorization: Bearer <redacted>' "
    f"-H 'Accept: application/json' -H 'Content-Type: application/json' -d '{json.dumps(payload)}'"
)
print("Equivalent curl:", curl_cmd)

try:
    # Disable redirects to detect HTML login redirects from the server
    response = requests.post(url, json=payload, headers=headers, timeout=30, allow_redirects=False)
except requests.RequestException as e:
    print("Request failed:", str(e))
    sys.exit(1)

print("Status code:", response.status_code)
print("Response headers:", dict(response.headers))
if response.history:
    print("Redirect history:", [ (r.status_code, r.headers.get('location')) for r in response.history ])
print("Response body:", response.text)

try:
    body = response.json()
except ValueError:
    body = None

if response.ok:
    print("✓ Process created successfully")
    if body is not None:
        print(body)
else:
    if response.status_code == 401:
        print("Unauthorized: check bearer token")
    elif response.status_code == 403:
        print("Forbidden: check token permissions")
    elif response.status_code == 409:
        print("Conflict: process may already exist")
    print("Error creating process:", body or response.text)
    sys.exit(1)

TOPIC_ID = "topic-1"
TOPIC_TITLE = "My First Topic"
TOPIC_CONTENT = "A short description"

# Optional bearer token from command line argument
bearer_token = sys.argv[1] if len(sys.argv) > 1 else None
headers = {}
if bearer_token:
    headers["Authorization"] = f"Bearer {bearer_token}"

headers.update({
    "Accept": "application/json",
    "Content-Type": "application/json",
})

payload = {
    "id": TOPIC_ID,
    "title": TOPIC_TITLE,
    "content": TOPIC_CONTENT,
}
url = f"{BASE_URL}/topic"

print("Creating topic...")
print("POST", url)
print("Payload:", payload)
print("Headers: Authorization: <redacted>" if bearer_token else "Headers: none (no auth)")

try:
    response = requests.post(url, json=payload, headers=headers, timeout=10)
except requests.RequestException as e:
    print("Request failed:", str(e))
    sys.exit(1)

print("Status code:", response.status_code)
print("Response headers:", dict(response.headers))
print("Response body:", response.text)

try:
    body = response.json()
except ValueError:
    body = None

if response.ok:
    print("✓ Topic created")
    if body:
        print(json.dumps(body, indent=2))
else:
    print("Error creating topic:", body or response.text)
    sys.exit(1)
