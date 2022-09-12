import os
from pprint import pprint
from databricks_api import DatabricksAPI
from db_session import mysql_session
from obfuscate_op import o2
import tables
import datetime

#os.environ["DATABRICKS_HOST"] = "https://xyz.azuredatabricks.net"
#os.environ["DATABRICKS_TOKEN"] = "dapi..."

os.environ["DATABRICKS_HOST"]="https://adb-3720311585959435.15.azuredatabricks.net"
os.environ["DATABRICKS_TOKEN"]="dapi866fb0277ee7a6a815481d98536e5f9e"

# Provide a host and token
db = DatabricksAPI(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

clusters = db.cluster.list_clusters()
mysql_session = mysql_session.getInstance()
session = mysql_session.get_db_session()

print("============ STARTED ==================")

index = 0


for cluster in clusters['clusters']:
    counter = 1
    next_page_exists = True
    page_size = 500
    offset = 0
    limit = page_size

    cluster_id = cluster['cluster_id']
    print(cluster_id)

    cluster_events = db.cluster.get_events(
        cluster_id=cluster_id,
        event_types=['RUNNING', 'RESIZING', 'TERMINATING'],
        offset=offset,
        limit=limit,
        order='ASC',
    )
    cluster_events = o2(cluster_events)

    while next_page_exists is True:
        next_page_exists = False
        if cluster_events is not None and 'events' in cluster_events.keys():
            events = cluster_events['events']
            #prev_min_workers = 0
            for cluster_event in events:
                #index = index+1
                # for cluster_event in cluster_events['clusters']:
                #pprint(index)
                #pprint(cluster_event)

                cluster_state = cluster_event['type']
                current_num_workers = (cluster_state == 'RUNNING' or cluster_state == 'RESIZING') \
                                      and cluster_event['details']['current_num_workers'] or 0
                target_num_workers = (cluster_state == 'RUNNING' or cluster_state == 'RESIZING') \
                                      and cluster_event['details']['target_num_workers'] or 0

                cluster_event_obj = tables.DBricksClusterEvents (
                    cluster_event_no = counter,
                    cluster_id = cluster_event['cluster_id'],
                    timestamp = cluster_event['timestamp'],
                    timestamp_dt = datetime.datetime.fromtimestamp(int(cluster_event['timestamp']/1000)),
                    cluster_state = cluster_state,
                    cluster_status_flag = cluster_state == 'TERMINATING' and False or True,
                    current_num_workers = current_num_workers,
                    target_num_workers = target_num_workers,
                    cluster_min_workers = (cluster_state == 'RESIZING' and 'autoscale' in cluster_event['details']['cluster_size'].keys())
                                          and cluster_event['details']['cluster_size']['autoscale']['min_workers'] or 0,
                    cluster_max_workers = (cluster_state == 'RESIZING' and 'autoscale' in cluster_event['details']['cluster_size'].keys())
                                          and cluster_event['details']['cluster_size']['autoscale']['max_workers'] or 0,
                )
                counter = counter+1
                session.add(cluster_event_obj)

            #session.commit()

            if 'next_page' in cluster_events.keys():
                next_page_exists = True

                offset = cluster_events['next_page']['offset']
                limit = cluster_events['next_page']['limit']
                cluster_events = db.cluster.get_events(
                    cluster_id=cluster_id,
                    event_types=['RUNNING', 'RESIZING', 'TERMINATING'],
                    offset=offset,
                    limit=limit,
                    order='ASC',
                )
                cluster_events = o2(cluster_events)
            else:
                next_page_exists = False

    session.commit()

        #counter = counter+1

session.commit()
print("============ COMPLETED ==================")

