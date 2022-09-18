import os
from pprint import pprint
from databricks_api import DatabricksAPI
from db_session import mysql_session
from obfuscate_op import o2
import tables
import datetime

#os.environ["DATABRICKS_HOST"] = "https://xyz.azuredatabricks.net"
#os.environ["DATABRICKS_TOKEN"] = "dapi..."

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


def fill_empty_events(from_time, to_time, counter, session, cluster_event, prev_cluster_min_workers, prev_cluster_max_workers):
    incr_time_step = 15 * 1000 #15 sec
    next_event_ts = from_time + incr_time_step
    while next_event_ts < to_time:
        cluster_event_obj = tables.DBricksClusterEvents(
            cluster_event_no=counter,
            cluster_id=cluster_event['cluster_id'],
            timestamp=next_event_ts,
            timestamp_dt=datetime.datetime.fromtimestamp(int(next_event_ts/1000)),
            cluster_state=cluster_state,
            cluster_status_flag=cluster_state == 'TERMINATING' and False or True,
            current_num_workers=0,
            target_num_workers=0,
            cluster_min_workers=prev_cluster_min_workers,
            cluster_max_workers=prev_cluster_max_workers,
        )
        counter = counter + 1
        next_event_ts = next_event_ts + incr_time_step
        session.add(cluster_event_obj)
    return counter


for cluster in clusters['clusters']:
    counter = 1
    next_page_exists = True
    page_size = 500
    offset = 0
    limit = page_size
    prev_cluster_min_workers = 0
    prev_cluster_max_workers = 0

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

    prev_event_state = None
    prev_event_ts = None
    prev_cluster_min_workers = 0
    prev_cluster_max_workers = 0

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

                cluster_min_workers = (cluster_state == 'RESIZING' and 'autoscale' in cluster_event['details']['cluster_size'].keys()) \
                                      and cluster_event['details']['cluster_size']['autoscale']['min_workers'] or prev_cluster_min_workers
                cluster_max_workers = (cluster_state == 'RESIZING' and 'autoscale' in cluster_event['details']['cluster_size'].keys()) \
                                      and cluster_event['details']['cluster_size']['autoscale']['max_workers'] or prev_cluster_max_workers

                cluster_min_workers = (cluster_state == 'TERMINATING') and 0 or cluster_min_workers
                cluster_max_workers = (cluster_state == 'TERMINATING') and 0 or cluster_max_workers

                if cluster_min_workers > 0 and prev_cluster_min_workers == 0:
                    prev_cluster_min_workers = cluster_min_workers

                if cluster_max_workers > 0 and prev_cluster_max_workers == 0:
                    prev_cluster_max_workers = cluster_max_workers

                if cluster_state == 'TERMINATING':
                    prev_event_state = cluster_state
                    prev_event_ts = cluster_event['timestamp']
                    prev_cluster_min_workers = cluster_min_workers
                    prev_cluster_max_workers = cluster_max_workers
                elif prev_event_state == 'TERMINATING' and cluster_state == 'RUNNING':
                    fill_empty_events(from_time=prev_event_ts, to_time=cluster_event['timestamp'], counter=counter, session=session,
                                      cluster_event=cluster_event, prev_cluster_min_workers=prev_cluster_min_workers,
                                      prev_cluster_max_workers=prev_cluster_max_workers)


                cluster_event_obj = tables.DBricksClusterEvents (
                        cluster_event_no = counter,
                        cluster_id = cluster_event['cluster_id'],
                        timestamp = cluster_event['timestamp'],
                        timestamp_dt = datetime.datetime.fromtimestamp(int(cluster_event['timestamp']/1000)),
                        cluster_state = cluster_state,
                        cluster_status_flag = cluster_state == 'TERMINATING' and False or True,
                        current_num_workers = current_num_workers,
                        target_num_workers = target_num_workers,
                        cluster_min_workers = cluster_min_workers,
                        cluster_max_workers = cluster_max_workers,
                    )
                counter = counter+1
                session.add(cluster_event_obj)


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

