import os

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient
from cron_descriptor import get_description
from obfuscate_op import o

#os.environ["DATABRICKS_HOST"] = "https://xyz.azuredatabricks.net"
#os.environ["DATABRICKS_TOKEN"] = "dapi..."

api_client = ApiClient(
  host = os.getenv('DATABRICKS_HOST'),
  token = os.getenv('DATABRICKS_TOKEN')
)


def get_jobs() -> dict:
    jobs_api = JobsApi(api_client)
    jobs_list = jobs_api.list_jobs()
    jobs_dict = {}
    cluster_jobs_dict = {}

    #print("Job name, Job ID")

    for job in jobs_list['jobs']:
        # pprint(job)

        job_settings = job['settings']
        job_id = job['job_id']
        #print(str(job_id) + ' || ' + job_settings['name'])

        if 'schedule' in job_settings.keys() and 'spark_python_task' in job_settings.keys():
            jobs_dict[job_id] = job

            if 'existing_cluster_id' in job_settings.keys():
                existing_cluster_id = job_settings['existing_cluster_id']
                #print('existing_cluster_id:' + existing_cluster_id)
                if existing_cluster_id not in cluster_jobs_dict.keys():
                    cluster_jobs_dict[existing_cluster_id] = [job_id]
                else:
                    cluster_jobs_dict[existing_cluster_id].append(job_id)

        elif 'existing_cluster_id' in job_settings.keys():
            jobs_dict[job_id] = job

            existing_cluster_id = job_settings['existing_cluster_id']
            #print('existing_cluster_id:' + existing_cluster_id)
            if existing_cluster_id not in cluster_jobs_dict.keys():
                cluster_jobs_dict[existing_cluster_id] = [job_id]
            else:
                cluster_jobs_dict[existing_cluster_id].append(job_id)
        elif 'new_cluster' in job_settings.keys():
            jobs_dict[job_id] = job
        #else:
            #pprint(job)
            #print("CHECK JOB DETAILS")

    return jobs_dict, cluster_jobs_dict


jobs_dict, cluster_jobs_dict = get_jobs()
#pprint(jobs_dict.keys())

clusters_api = ClusterApi(api_client)
clusters_list = clusters_api.list_clusters()
clusters_dict = {}

for cluster in clusters_list['clusters']:
    cluster_id = cluster['cluster_id']
    clusters_dict[cluster_id] = cluster

print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print("Cluster Name, Cluster State, Min Node(s), Max Node(s), Auto-terminate after (mins)")
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
for cluster in clusters_list['clusters']:
    cluster_id = cluster['cluster_id']

    if cluster_id is not None:
        cluster = clusters_dict[cluster_id]
        if 'num_workers' in cluster.keys():
            print(o(cluster['cluster_name']), ',', cluster['state'], ',', cluster['driver_node_type_id'], ',', cluster['num_workers'], ',', cluster['num_workers'], ',', cluster['autotermination_minutes'])
        else:
            print(o(cluster['cluster_name']), ',', cluster['state'], ',', cluster['driver_node_type_id'], ',', cluster['autoscale']['min_workers'], ',', cluster['autoscale']['max_workers'], ',', cluster['autotermination_minutes'])
        print("++++++++++++ JOBS ++++++++++++++")
    #print(f"{cluster['cluster_name']}, {cluster['cluster_id']}")

    default_tags = cluster['default_tags']
    if 'JobId' in default_tags:
        job_id = int(default_tags['JobId'])
        #print(job_id)
        #print(type(job_id)) #str
        if job_id in jobs_dict.keys():
            job_settings = jobs_dict[job_id]['settings']
            job_schedule = job_settings['schedule']['quartz_cron_expression']

            #print(jobs_dict[job_id]['settings']['name'] + ',' + jobs_dict[job_id]['creator_user_name'])
            print(o(jobs_dict[job_id]['creator_user_name'])
                  + ' || ' + jobs_dict[job_id]['settings']['schedule']['pause_status']
                  + ' || ' + get_description(job_schedule)
                  + ' || ' + 'Job'
                  + ' || ' + o(jobs_dict[job_id]['settings']['name']))
        else:
            print('job_id not found in jobs_dict')
    else:
        existing_cluster_id = cluster['cluster_id']
        if existing_cluster_id in cluster_jobs_dict.keys():
            list_of_jobs = cluster_jobs_dict[existing_cluster_id]
            for job_id in list_of_jobs:
                #pprint(jobs_dict[job_id])

                job_settings = jobs_dict[job_id]['settings']
                if 'schedule' in job_settings.keys():
                    job_schedule = job_settings['schedule']['quartz_cron_expression']

                    print(o(jobs_dict[job_id]['creator_user_name'])
                        + ' || ' + jobs_dict[job_id]['settings']['schedule']['pause_status']
                        + ' || ' + get_description(job_schedule)
                        + ' || ' + 'UI'
                        + ' || ' + o(jobs_dict[job_id]['settings']['name']))
                else:
                    print(o(jobs_dict[job_id]['creator_user_name'])
                        + ' || ' + 'NA'
                        + ' || ' + 'NA'
                        + ' || ' + 'NA'
                        + ' || ' + o(jobs_dict[job_id]['settings']['name']))

        else:
            print("DOESN'T RUN ANY JOB")

    print("-----------------------")


