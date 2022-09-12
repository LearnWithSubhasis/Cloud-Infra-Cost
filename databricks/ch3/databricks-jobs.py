import os
from pprint import pprint

from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient
from cron_descriptor import get_description
from db_session import mysql_session

from obfuscate_op import o2
import tables

#os.environ["DATABRICKS_HOST"] = "https://xyz.azuredatabricks.net"
#os.environ["DATABRICKS_TOKEN"] = "dapi..."

api_client = ApiClient(
  host = os.getenv('DATABRICKS_HOST'),
  token = os.getenv('DATABRICKS_TOKEN')
)

jobs_api = JobsApi(api_client)
jobs_list = o2(jobs_api.list_jobs())

mysql_session = mysql_session.getInstance()
session = mysql_session.get_db_session()

def parse_cron(schedule_desc):
  schedule_day_of_week = 'NA'
  schedule_hour_of_day = -1
  schedule_min_of_hour = -1

  if schedule_desc is not None:
    sch_list = schedule_desc.split(" ")
    time = ""
    am_pm = ""
    index = 0
    for index in range(0, len(sch_list)):
      if sch_list[index] == 'At':
        time = sch_list[index+1]
        am_pm = sch_list[index+2]
        time_list = time.split(":")
        schedule_hour_of_day = int(time_list[0])
        schedule_min_of_hour = int(time_list[1])
        if am_pm == 'PM':
          schedule_hour_of_day = schedule_hour_of_day + 12

      elif sch_list[index] == 'on':
        schedule_day_of_week = sch_list[index+1]

      if schedule_hour_of_day >= 0 and schedule_day_of_week == 'NA':
        schedule_day_of_week = 'Everyday'

  return schedule_day_of_week, schedule_hour_of_day, schedule_min_of_hour


for job in jobs_list['jobs']:
    quartz_cron_expression = 'schedule' in job['settings'].keys() and job['settings']['schedule'][
        'quartz_cron_expression'] or None
    schedule_desc = quartz_cron_expression is not None and get_description(quartz_cron_expression) or None
    schedule_day_of_week, schedule_hour_of_day, schedule_min_of_hour = parse_cron(schedule_desc)

    job_obj = tables.DBricksJobs (
        job_id = job['job_id'],
        job_name = job['settings']['name'],
        creator_user_name = 'creator_user_name' in job.keys() and job['creator_user_name'] or None,
        existing_cluster_id = 'existing_cluster_id' in job['settings'].keys() and job['settings']['existing_cluster_id'] or None,
        created_time=job['created_time'],
        pause_status = 'schedule' in job['settings'].keys() and job['settings']['schedule']['pause_status'] or None,
        quartz_cron_expression = quartz_cron_expression,
        schedule_desc = schedule_desc,
        schedule_day_of_week = schedule_day_of_week,
        schedule_hour_of_day = schedule_hour_of_day,
        schedule_min_of_hour = schedule_min_of_hour,
      )
    session.add(job_obj)

session.commit()




