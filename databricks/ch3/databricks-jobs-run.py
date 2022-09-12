import os
from pprint import pprint

from databricks_api import DatabricksAPI

os.environ["DATABRICKS_HOST"] = "https://adb-3720311585959435.15.azuredatabricks.net"
os.environ["DATABRICKS_TOKEN"] = "dapi866fb0277ee7a6a815481d98536e5f9e"

# Provide a host and token
db = DatabricksAPI(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

jobs = db.jobs.list_jobs(
    job_type=None,
    expand_tasks=None,
    limit=None,
    offset=None,
    headers=None,
    version=None,
)

#pprint(jobs)
print("=========================")

job_runs = db.jobs.list_runs(
    job_id=18,
    active_only=None,
    completed_only=None,
    offset=None,
    limit=None,
    headers=None,
    version=None,
)

pprint(job_runs)

print("=========================")

job_runs = db.jobs.list_runs(
    job_id=461437252122719,
    active_only=None,
    completed_only=None,
    offset=None,
    limit=None,
    headers=None,
    version=None,
)

pprint(job_runs)

