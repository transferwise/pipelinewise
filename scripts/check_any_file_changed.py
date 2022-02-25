import json
import os
import sys

import requests

with open(os.environ["GITHUB_EVENT_PATH"], mode="r", encoding="utf-8") as f:
    gh_event_data = json.load(f)
    PR_URL = gh_event_data["pull_request"]["url"]

with requests.get(
    f"{PR_URL}/files", headers={"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}"}
) as resp:
    files_changed = [f["filename"] for f in resp.json()]

for f in sys.argv[1:]:
    if f in files_changed:
        sys.exit(0)

sys.exit(1)
