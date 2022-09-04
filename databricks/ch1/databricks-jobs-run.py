import os
from pprint import pprint

from databricks_api import DatabricksAPI
from obfuscate_op import o2

#os.environ["DATABRICKS_HOST"] = "https://xyz.azuredatabricks.net"
#os.environ["DATABRICKS_TOKEN"] = "dapi..."

# Provide a host and token
db = DatabricksAPI(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

jobs = db.jobs.list_jobs(
    job_type=None,
    expand_tasks=True,
    limit=None,
    offset=None,
    headers=None,
    version=None,
)

#pprint(jobs)
jobs_obfus = o2(jobs)
pprint(jobs_obfus)
print("=========================")

# job_runs = db.jobs.list_runs(
#     job_id=18,
#     active_only=None,
#     completed_only=None,
#     offset=None,
#     limit=None,
#     headers=None,
#     version=None,
# )
#
# pprint(job_runs)
#
# print("=========================")
#
# job_runs = db.jobs.list_runs(
#     job_id=461437252122719,
#     active_only=None,
#     completed_only=None,
#     offset=None,
#     limit=None,
#     headers=None,
#     version=None,
# )
#
# pprint(job_runs)

