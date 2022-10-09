import os
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.sdk.api_client import ApiClient
from obfuscate_op import o, o2
import tables
from db_session import mysql_session

#os.environ["DATABRICKS_HOST"] = "https://xyz.azuredatabricks.net"
#os.environ["DATABRICKS_TOKEN"] = "dapi..."

api_client = ApiClient(
  host = os.getenv('DATABRICKS_HOST'),
  token = os.getenv('DATABRICKS_TOKEN')
)

clusters_api = ClusterApi(api_client)
clusters_list = o2(clusters_api.list_clusters())

clusters_api.list_clusters()

mysql_session = mysql_session.getInstance()
session = mysql_session.get_db_session()

for cluster in clusters_list['clusters']:
  spark_version_str = cluster['spark_version']
  spark_version_str = (spark_version_str[:48] + '..') if len(spark_version_str) > 50 else spark_version_str
  print(cluster['cluster_id'])
  cluster_obj = tables.DBricksClusters(
    cluster_id = cluster['cluster_id'],
    cluster_name = cluster['cluster_name'],
    cluster_source = cluster['cluster_source'],
    autotermination_minutes = cluster['autotermination_minutes'],
    creator_user_name = cluster['creator_user_name'],
    node_type_id = cluster['node_type_id'],
    driver_node_type_id = cluster['driver_node_type_id'],
    spot_bid_max_price = 'azure_attributes' in cluster.keys() and cluster['azure_attributes']['spot_bid_max_price'] or -1,
    runtime_engine = 'runtime_engine' in cluster.keys() and cluster['runtime_engine'] or 'None',
    state = cluster['state'],
    start_time = cluster['start_time'],
    terminated_time = 'terminated_time' in cluster.keys() and cluster['terminated_time'] or None,
    spark_version = spark_version_str,
    min_workers = 'autoscale' in cluster.keys() and cluster['autoscale']['min_workers'] or cluster['num_workers'],
    max_workers = 'autoscale' in cluster.keys() and cluster['autoscale']['max_workers'] or cluster['num_workers'],
    existing_cluster=True
  )
  session.add(cluster_obj)

session.commit()



