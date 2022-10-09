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

def create_cluster_events(job_start_time, job_end_time, cluster_id, min_workers):
    event_counter = 1
    cluster_event_exists = session.query(
        session.query(tables.DBricksClusterEvents)
        .filter_by(cluster_id=cluster_id, timestamp=job_start_time, cluster_event_no=event_counter).exists()
    ).scalar()

    if cluster_event_exists is False:
        cluster_event_obj = tables.DBricksClusterEvents(
            cluster_event_no=event_counter,
            cluster_id=cluster_id,
            timestamp=job_start_time,
            timestamp_dt=datetime.datetime.fromtimestamp(int(job_start_time / 1000)),
            cluster_state='RUNNING',
            cluster_status_flag=True,
            current_num_workers=min_workers,
            target_num_workers=min_workers,
            cluster_min_workers=min_workers,
            cluster_max_workers=min_workers,
        )
        event_counter = event_counter + 1
        session.add(cluster_event_obj)

        cluster_event_obj = tables.DBricksClusterEvents(
            cluster_event_no=event_counter,
            cluster_id=cluster_id,
            timestamp=job_end_time,
            timestamp_dt=datetime.datetime.fromtimestamp(int(job_end_time / 1000)),
            cluster_state='TERMINATING',
            cluster_status_flag=False,
            current_num_workers=min_workers,
            target_num_workers=min_workers,
            cluster_min_workers=min_workers,
            cluster_max_workers=min_workers,
        )
        session.add(cluster_event_obj)
        session.commit()


'''
 'cluster_instance': {'cluster_id': '0906-180704-cf0518b6',
                      'spark_context_id': '2592005139220461570'},
 'cluster_spec': {'libraries': [{'jar': 'dbfs:/etl/int/lib/euclid-etl-tasks-gb-az-1.0.143.jar'}],
                  'new_cluster': {'azure_attributes': {'availability': 'ON_DEMAND_AZURE'},
                                  'cluster_log_conf': {'dbfs': {'destination': 'dbfs:/mnt/spark-logs/gb/2022-09-06/ciduswoverridehandlerwithlocking'}},
                                  'custom_tags': {'Smart_ID': 'SF_GB'},
                                  'enable_elastic_disk': True,
                                  'node_type_id': 'Standard_D16s_v3',
                                  'num_workers': 1,
                                  'spark_env_vars': {'AZURE_CLIENT_ID': '{{secrets/euclid-ccm/AZURE_CLIENT_ID}}',
                                                     'AZURE_CLIENT_SECRET': '{{secrets/euclid-ccm/AZURE_CLIENT_SECRET}}',
                                                     'AZURE_TENANT_ID': '{{secrets/euclid-ccm/AZURE_TENANT_ID}}'},
                                  'spark_version': 'apache-spark-2.4.x-esr-scala2.11'}},
'''
def upsert_cluster_info(job_run):
    cluster_id = job_run['cluster_instance']['cluster_id']
    existing_cluster = True
    min_workers = 0
    cluster_exists = session.query(
        session.query(tables.DBricksClusters).filter_by(cluster_id=cluster_id).exists()
    ).scalar()

    if cluster_exists is False and 'new_cluster' in job_run['cluster_spec'].keys():
        existing_cluster = False
        new_cluster = job_run['cluster_spec']['new_cluster']

        spark_version_str = new_cluster['spark_version']
        spark_version_str = (spark_version_str[:48] + '..') if len(spark_version_str) > 50 else spark_version_str
        min_workers = new_cluster['num_workers']

        cluster_obj = tables.DBricksClusters(
            cluster_id = cluster_id,
            cluster_name = 'job-' + str(job_run['job_id']) + '-run-' + str(job_run['run_id']),
            cluster_source = 'JOB',
            autotermination_minutes = 0,
            creator_user_name = job_run['creator_user_name'],
            node_type_id = new_cluster['node_type_id'],
            driver_node_type_id = new_cluster['node_type_id'],
            spot_bid_max_price = -1,
            runtime_engine = 'None',
            state = None,
            start_time = job_run['start_time'],
            terminated_time = 'end_time' in job_run.keys() and job_run['end_time'] or None,
            spark_version = spark_version_str,
            min_workers = min_workers,
            max_workers = min_workers,
            existing_cluster=False
        )
        session.add(cluster_obj)
        session.commit()
    return cluster_id, existing_cluster, min_workers


def upsert_job_info(job_run, cluster_id):
    job_id = job_run['job_id']
    existing_job = session.query(
        session.query(tables.DBricksJobs).filter_by(job_id=job_id).exists()
    ).scalar()

    if existing_job is False:
        dt_object = datetime.datetime.fromtimestamp(int(job_run['start_time'] / 1000))
        job_obj = tables.DBricksJobs(
            job_id=job_id,
            job_name=job_run['run_name'],
            job_creator_user_name=job_run['creator_user_name'],
            existing_cluster_id=cluster_id,
            job_created_time=job_run['start_time'],
            pause_status=None,
            quartz_cron_expression=None,
            schedule_desc=dt_object,
            schedule_day_of_week=dt_object.strftime('%A'),
            schedule_hour_of_day=dt_object.strftime('%H'),
            schedule_min_of_hour=dt_object.strftime('%M'),
        )
        session.add(job_obj)
        session.commit()
    return job_id, existing_cluster


def check_if_valid_run(job_run, tenant_pattern=None):
    if tenant_pattern is None or len(tenant_pattern) == 0:
        return True

    job_params = 'spark_jar_task' in job_run['task'].keys() and job_run['task']['spark_jar_task']['parameters'] or None
    if job_params is not None:
        for job_param in job_params:
            if tenant_pattern in job_param:
                return True
    return False


TENANT_PATTERN = 'cn'
counter = 1
next_page_exists = True
page_size = 500
offset = 0
limit = page_size

job_runs = db.jobs.list_runs(
    job_id=None,
    active_only=None,
    completed_only=None,
    offset=offset,
    limit=limit,
    headers=None,
    version=None,
)

while next_page_exists is True:
    offset = offset + page_size
    next_page_exists = False
    if job_runs is not None and 'runs' in job_runs.keys():
        runs = job_runs['runs']
        for job_run in runs:
            is_valid_run = check_if_valid_run(job_run, TENANT_PATTERN)
            if is_valid_run is True:
                print(job_run['run_name'])
                pprint(job_run)
                job_run_status_str = job_run['state']['life_cycle_state']
                cluster_id, existing_cluster, min_workers = upsert_cluster_info(job_run)
                job_id, existing_job = upsert_job_info(job_run, cluster_id)
                job_state_message = 'state_message' in job_run['state'].keys() and job_run['state']['state_message'] or ""
                job_state_message = len(job_state_message) >= 400 and job_state_message[:397] + '..' or job_state_message

                job_run_obj = tables.DBricksJobsRun (
                    run_event_no=counter,
                    job_id=job_run['job_id'],
                    run_id=job_run['run_id'],
                    job_run_name=job_run['run_name'],
                    job_cluster_id=cluster_id,
                    job_start_time=job_run['start_time'],
                    job_start_time_dt=datetime.datetime.fromtimestamp(int(job_run['start_time'] / 1000)),
                    job_end_time=job_run['end_time'],
                    job_end_time_dt=datetime.datetime.fromtimestamp(int(job_run['end_time'] / 1000)),
                    job_execution_duration=job_run['execution_duration']/60000,
                    job_setup_duration=job_run['setup_duration']/60000,
                    job_result_state='result_state' in job_run['state'].keys() and job_run['state']['result_state'] or None,
                    job_life_cycle_state=job_run_status_str,
                    job_is_running=True,
                    job_state_message=job_state_message,
                    job_user_cancelled_or_timeout='user_cancelled_or_timeout' in job_run['state'].keys() and job_run['state']['user_cancelled_or_timeout'] or False,
                    job_run_page_url=job_run['run_page_url'],
                    job_run_type=job_run['run_type'],
                )
                counter = counter + 1
                session.add(job_run_obj)

                ######
                # Create two events in databricks_cluster_events
                ######
                create_cluster_events(job_run['start_time'], job_run['end_time'], cluster_id, min_workers)



    if 'has_more' in job_runs.keys() and job_runs['has_more'] is True:
        next_page_exists = True
        job_runs = db.jobs.list_runs(
            job_id=None,
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



# job_runs = db.jobs.list_runs(
#     job_id=None,
#     active_only=None,
#     completed_only=None,
#     offset=None,
#     limit=500,
#     headers=None,
#     version=None,
# )
#
# #pprint(job_runs)
# # 'task': {'spark_jar_task': {'jar_uri': '',
# #                                        'main_class_name': 'com.walmartlabs.euclid.etl.core.ETLCoreApp',
# #                                        'parameters'
# for job_run in job_runs['runs']:
#     job_params = job_run['task']['spark_jar_task']['parameters']
#     for job_param in job_params:
#         #print(job_param)
#         if 'gb' in job_param:
#             pprint(job_run)
#             print("===============")


# mysql_session = mysql_session.getInstance()
# session = mysql_session.get_db_session()
#
# jobs = db.jobs.list_jobs(
#     job_type=None,
#     expand_tasks=None,
#     limit=None,
#     offset=None,
#     headers=None,
#     version=None,
# )
#
# #pprint(jobs)
# print("=========================")
#
# index = 0
# for job in jobs['jobs']:
#     job_id = job['job_id']
#     print(job_id)
#
#     counter = 1
#     next_page_exists = True
#     page_size = 500
#     offset = 0
#     limit = page_size
#
#     job_runs = db.jobs.list_runs(
#         job_id=job_id,
#         active_only=None,
#         completed_only=None,
#         offset=offset,
#         limit=limit,
#         headers=None,
#         version=None,
#     )
#
#     while next_page_exists is True:
#         next_page_exists = False
#         if job_runs is not None and 'runs' in job_runs.keys():
#             runs = job_runs['runs']
#             for job_run in runs:
#                 job_run_status_str = job_run['state']['life_cycle_state']
#
#                 # start_time = job_run['start_time'] - 1
#                 # end_time = job_run['end_time'] + 1
#                 # job_run_obj = tables.DBricksJobsRun(
#                 #     run_event_no=counter,
#                 #     job_id=job_run['job_id'],
#                 #     run_id=job_run['run_id'],
#                 #     job_run_name=job_run['run_name'],
#                 #     # job_cluster_id='cluster_instance' in job_run.keys() and job_run['cluster_instance']['cluster_id'] or 'NA',
#                 #     job_start_time=start_time,
#                 #     job_start_time_dt=datetime.datetime.fromtimestamp(int(start_time / 1000)),
#                 #     job_end_time=job_run['start_time'],
#                 #     job_end_time_dt=datetime.datetime.fromtimestamp(int(start_time / 1000)),
#                 #     job_execution_duration=0,
#                 #     job_setup_duration=0,
#                 #     job_result_state='NA',
#                 #     job_life_cycle_state='NA',
#                 #     job_is_running=False,
#                 #     job_state_message='NA',
#                 #     job_user_cancelled_or_timeout=False,
#                 #     job_run_page_url='NA',
#                 #     job_run_type='NA',
#                 # )
#                 # counter = counter + 1
#                 # session.add(job_run_obj)
#
#                 job_run_obj = tables.DBricksJobsRun (
#                     run_event_no=counter,
#                     job_id=job_run['job_id'],
#                     run_id=job_run['run_id'],
#                     job_run_name=job_run['run_name'],
#                     #job_cluster_id='cluster_instance' in job_run.keys() and job_run['cluster_instance']['cluster_id'] or 'NA',
#                     job_start_time=job_run['start_time'],
#                     job_start_time_dt=datetime.datetime.fromtimestamp(int(job_run['start_time'] / 1000)),
#                     job_end_time=job_run['end_time'],
#                     job_end_time_dt=datetime.datetime.fromtimestamp(int(job_run['end_time'] / 1000)),
#                     job_execution_duration=job_run['execution_duration']/60000,
#                     job_setup_duration=job_run['setup_duration']/60000,
#                     job_result_state=job_run['state']['result_state'],
#                     job_life_cycle_state=job_run_status_str,
#                     job_is_running=True,
#                     job_state_message=job_run['state']['state_message'],
#                     job_user_cancelled_or_timeout='user_cancelled_or_timeout' in job_run['state'].keys() and job_run['state']['user_cancelled_or_timeout'] or False,
#                     job_run_page_url=job_run['run_page_url'],
#                     job_run_type=job_run['run_type'],
#                 )
#                 counter = counter + 1
#                 session.add(job_run_obj)
#
#                 # job_run_obj = tables.DBricksJobsRun (
#                 #     run_event_no=counter,
#                 #     job_id=job_run['job_id'],
#                 #     run_id=job_run['run_id'],
#                 #     job_run_name=job_run['run_name'],
#                 #     #job_cluster_id='cluster_instance' in job_run.keys() and job_run['cluster_instance']['cluster_id'] or 'NA',
#                 #     job_start_time=job_run['end_time'],
#                 #     job_start_time_dt=datetime.datetime.fromtimestamp(int(job_run['end_time'] / 1000)),
#                 #     job_end_time=job_run['end_time'],
#                 #     job_end_time_dt=datetime.datetime.fromtimestamp(int(job_run['end_time'] / 1000)),
#                 #     job_execution_duration=job_run['execution_duration']/60000,
#                 #     job_setup_duration=job_run['setup_duration']/60000,
#                 #     job_result_state=job_run['state']['result_state'],
#                 #     job_life_cycle_state=job_run_status_str,
#                 #     job_is_running=True,
#                 #     job_state_message=job_run['state']['state_message'],
#                 #     job_user_cancelled_or_timeout='user_cancelled_or_timeout' in job_run['state'].keys() and job_run['state']['user_cancelled_or_timeout'] or False,
#                 #     job_run_page_url=job_run['run_page_url'],
#                 #     job_run_type=job_run['run_type'],
#                 # )
#                 # counter = counter + 1
#                 # session.add(job_run_obj)
#                 #
#                 # job_run_obj = tables.DBricksJobsRun(
#                 #     run_event_no=counter,
#                 #     job_id=job_run['job_id'],
#                 #     run_id=job_run['run_id'],
#                 #     job_run_name=job_run['run_name'],
#                 #     # job_cluster_id='cluster_instance' in job_run.keys() and job_run['cluster_instance']['cluster_id'] or 'NA',
#                 #     job_start_time=end_time,
#                 #     job_start_time_dt=datetime.datetime.fromtimestamp(int(end_time / 1000)),
#                 #     job_end_time=end_time,
#                 #     job_end_time_dt=datetime.datetime.fromtimestamp(int(end_time / 1000)),
#                 #     job_execution_duration=0,
#                 #     job_setup_duration=0,
#                 #     job_result_state='NA',
#                 #     job_life_cycle_state='NA',
#                 #     job_is_running=False,
#                 #     job_state_message='NA',
#                 #     job_user_cancelled_or_timeout=False,
#                 #     job_run_page_url='NA',
#                 #     job_run_type='NA',
#                 # )
#                 # counter = counter + 1
#                 # session.add(job_run_obj)
#
#             if 'has_more' in job_runs.keys() and job_runs['has_more'] == True:
#                 next_page_exists = True
#                 offset = job_runs['next_page']['offset']
#                 limit = job_runs['next_page']['limit']
#                 job_runs = db.jobs.list_runs(
#                     job_id=job_id,
#                     active_only=None,
#                     completed_only=None,
#                     offset=offset,
#                     limit=limit,
#                     headers=None,
#                     version=None,
#                 )
#
#                 job_runs = o2(job_runs)
#             else:
#                 next_page_exists = False
#
#     session.commit()
# session.commit()



