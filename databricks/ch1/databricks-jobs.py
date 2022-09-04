import os
from pprint import pprint

from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.runs.api import RunsApi
from cron_descriptor import get_description
from obfuscate_op import o2

#os.environ["DATABRICKS_HOST"] = "https://xyz.azuredatabricks.net"
#os.environ["DATABRICKS_TOKEN"] = "dapi..."

api_client = ApiClient(
  host = os.getenv('DATABRICKS_HOST'),
  token = os.getenv('DATABRICKS_TOKEN')
)

jobs_api = JobsApi(api_client)
jobs_list = jobs_api.list_jobs()

print("Job name, Job ID")

for job in jobs_list['jobs']:
    pprint(o2(job))

    job_settings = job['settings']
    existing_cluster_id = None
    new_cluster_id = None
    cluster_id = None

    if 'existing_cluster_id' in job_settings.keys():
        existing_cluster_id = job_settings['existing_cluster_id']

    if 'new_cluster' in job_settings.keys():
        new_cluster_id = job_settings['new_cluster']

    if 'schedule' in job_settings.keys():
        print(job_settings['schedule']['quartz_cron_expression'])

    print("-----------------------")


runs_api = RunsApi(api_client)
job_runs = runs_api.list_runs(
    job_id=18,
    active_only=None,
    completed_only=None,
    offset=None,
    limit=None,
)
pprint(o2(job_runs))

print("==============================")
job_run = runs_api.get_run(run_id=122931)
pprint(o2(job_run))

# print("==============================")
# job_run_op = runs_api.get_run_output(run_id=122931)
# pprint(job_run_op)


