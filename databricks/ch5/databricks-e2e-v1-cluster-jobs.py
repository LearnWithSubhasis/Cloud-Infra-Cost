# import os
# from databricks_cli.clusters.api import ClusterApi
# from databricks_cli.jobs.api import JobsApi
# from databricks_cli.sdk.api_client import ApiClient
# from db_session import mysql_session
# from obfuscate_op import o2
# import tables
#
# #os.environ["DATABRICKS_HOST"] = "https://xyz.azuredatabricks.net"
# #os.environ["DATABRICKS_TOKEN"] = "dapi..."
#
# api_client = ApiClient(
#   host = os.getenv('DATABRICKS_HOST'),
#   token = os.getenv('DATABRICKS_TOKEN')
# )
#
# mysql_session = mysql_session.getInstance()
# session = mysql_session.get_db_session()
#
# def get_jobs() -> dict:
#     jobs_api = JobsApi(api_client)
#     jobs_list = jobs_api.list_jobs()
#     jobs_dict = {}
#     cluster_jobs_dict = {}
#
#     #print("Job name, Job ID")
#
#     for job in jobs_list['jobs']:
#         # pprint(job)
#
#         job_settings = job['settings']
#         job_id = job['job_id']
#         #print(str(job_id) + ' || ' + job_settings['name'])
#
#         if 'schedule' in job_settings.keys() and 'spark_python_task' in job_settings.keys():
#             jobs_dict[job_id] = job
#
#             if 'existing_cluster_id' in job_settings.keys():
#                 existing_cluster_id = job_settings['existing_cluster_id']
#                 #print('existing_cluster_id:' + existing_cluster_id)
#                 if existing_cluster_id not in cluster_jobs_dict.keys():
#                     cluster_jobs_dict[existing_cluster_id] = [job_id]
#                 else:
#                     cluster_jobs_dict[existing_cluster_id].append(job_id)
#
#         elif 'existing_cluster_id' in job_settings.keys():
#             jobs_dict[job_id] = job
#
#             existing_cluster_id = job_settings['existing_cluster_id']
#             #print('existing_cluster_id:' + existing_cluster_id)
#             if existing_cluster_id not in cluster_jobs_dict.keys():
#                 cluster_jobs_dict[existing_cluster_id] = [job_id]
#             else:
#                 cluster_jobs_dict[existing_cluster_id].append(job_id)
#         elif 'new_cluster' in job_settings.keys():
#             jobs_dict[job_id] = job
#         #else:
#             #pprint(job)
#             #print("CHECK JOB DETAILS")
#
#     return jobs_dict, cluster_jobs_dict
#
#
# jobs_dict, cluster_jobs_dict = o2(get_jobs())
# # #pprint(jobs_dict.keys())
#
# clusters_api = ClusterApi(api_client)
# clusters_list = o2(clusters_api.list_clusters())
# clusters_dict = {}
#
# for cluster in clusters_list['clusters']:
#     cluster_id = cluster['cluster_id']
#     clusters_dict[cluster_id] = cluster
#
# counter = 1
# for cluster in clusters_list['clusters']:
#     cluster_id = cluster['cluster_id']
#     print(cluster_id, counter)
#     counter += 1
#
#     if cluster_id is not None:
#         #cluster = clusters_dict[cluster_id]
#         default_tags = cluster['default_tags']
#         if 'JobId' in default_tags:
#             job_id_v = int(default_tags['JobId'])
#             cluster_job_obj = tables.DBricksClusterJobs (
#                 job_id = job_id_v,
#                 cluster_id = cluster_id,
#             )
#             session.add(cluster_job_obj)
#             #pprint(job_id_v)
#         else:
#             #pprint('outside')
#             existing_cluster_id = cluster['cluster_id']
#             if existing_cluster_id in cluster_jobs_dict.keys():
#                 list_of_jobs = cluster_jobs_dict[existing_cluster_id]
#                 for job_id in list_of_jobs:
#                     cluster_job_obj = tables.DBricksClusterJobs(
#                         job_id=int(job_id),
#                         cluster_id=cluster_id,
#                     )
#                     session.add(cluster_job_obj)
#                     #pprint(job_id)
#
# session.commit()
#
# print("========= COMPLETED =========")