import os
import datetime
from pprint import pprint
from databricks_api import DatabricksAPI
from db_session import mysql_session
from obfuscate_op import o2
import tables

#os.environ["DATABRICKS_HOST"] = "https://xyz.azuredatabricks.net"
#os.environ["DATABRICKS_TOKEN"] = "dapi..."

# Provide a host and token
db = DatabricksAPI(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

mysql_session = mysql_session.getInstance()
session = mysql_session.get_db_session()

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

index = 0
for job in jobs['jobs']:
    job_id = job['job_id']
    print(job_id)

    counter = 1
    next_page_exists = True
    page_size = 10
    offset = 0
    limit = page_size

    job_runs = db.jobs.list_runs(
        job_id=job_id,
        active_only=None,
        completed_only=None,
        offset=offset,
        limit=limit,
        headers=None,
        version=None,
    )

    pprint(job_runs)

    while next_page_exists is True:
        next_page_exists = False
        if job_runs is not None and 'runs' in job_runs.keys():
            runs = job_runs['runs']
            for job_run in runs:
                job_run_status_str = job_run['state']['life_cycle_state']

                job_run_obj = tables.DBricksJobsRun (
                    run_event_no=counter,
                    job_id=job_run['job_id'],
                    run_id=job_run['run_id'],
                    job_run_name=job_run['run_name'],
                    #job_cluster_id='cluster_instance' in job_run.keys() and job_run['cluster_instance']['cluster_id'] or 'NA',
                    job_start_time=job_run['start_time'],
                    job_start_time_dt=datetime.datetime.fromtimestamp(int(job_run['start_time'] / 1000)),
                    job_end_time=job_run['end_time'],
                    job_end_time_dt=datetime.datetime.fromtimestamp(int(job_run['end_time'] / 1000)),
                    job_execution_duration=job_run['execution_duration']/60000,
                    job_setup_duration=job_run['setup_duration']/60000,
                    job_result_state=job_run['state']['result_state'],
                    job_life_cycle_state=job_run_status_str,
                    job_is_running=True,
                    job_state_message=job_run['state']['state_message'],
                    job_user_cancelled_or_timeout='user_cancelled_or_timeout' in job_run['state'].keys() and job_run['state']['user_cancelled_or_timeout'] or False,
                    job_run_page_url=job_run['run_page_url'],
                    job_run_type=job_run['run_type'],
                )
                counter = counter + 1
                session.add(job_run_obj)


            if 'has_more' in job_runs.keys() and job_runs['has_more'] == True:
                next_page_exists = True
                offset = job_runs['next_page']['offset']
                limit = job_runs['next_page']['limit']
                job_runs = db.jobs.list_runs(
                    job_id=job_id,
                    active_only=None,
                    completed_only=None,
                    offset=offset,
                    limit=limit,
                    headers=None,
                    version=None,
                )

                job_runs = o2(job_runs)
            else:
                next_page_exists = False

    session.commit()
session.commit()



