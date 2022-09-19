#from pymysql import Timestamp
from sqlalchemy import create_engine, Column, Integer, String, Float, BigInteger, ForeignKey, PrimaryKeyConstraint, \
   TIMESTAMP, Boolean, DateTime
from db_session import mysql_session

"""
CREATE DATABASE cloud_infra_db CHARACTER SET utf8;
-- CREATE USER 'cloud_infra_user' IDENTIFIED BY 'cloud_infra_pass';
GRANT ALL PRIVILEGES ON cloud_infra_db.* TO 'cloud_infra_user';
"""

"""
References:
https://docs.sqlalchemy.org/en/14/core/type_basics.html
https://azure.microsoft.com/en-in/pricing/details/virtual-machines/windows/#pricing
"""

mysql_session = mysql_session.getInstance()
Base, engine = mysql_session.get_db_Base()

class DBricksClusters(Base):
   __tablename__ = 'databricks_clusters'
   cluster_id = Column(String(30), primary_key=True)

   cluster_name = Column(String(50))
   cluster_source = Column(String(5))
   autotermination_minutes = Column(Integer)
   creator_user_name = Column(String(50))
   node_type_id = Column(String(20))
   driver_node_type_id = Column(String(20))
   spot_bid_max_price = Column(Float)
   runtime_engine = Column(String(15))
   state = Column(String(15))
   start_time = Column(BigInteger)
   terminated_time = Column(BigInteger)
   spark_version = Column(String(30))
   min_workers = Column(Integer)
   max_workers = Column(Integer)

class DBricksJobs(Base):
   __tablename__ = 'databricks_jobs'
   job_id = Column(String(30), primary_key=True)

   job_name = Column(String(100))
   existing_cluster_id = Column(String(30))
   job_created_time = Column(BigInteger)
   job_creator_user_name = Column(String(50))
   pause_status = Column(String(20))
   quartz_cron_expression = Column(String(20))
   schedule_desc = Column(String(50))
   schedule_day_of_week = Column(String(10))
   schedule_hour_of_day = Column(Integer)
   schedule_min_of_hour = Column(Integer)

class DBricksClusterJobs(Base):
   __tablename__ = 'databricks_cluster_jobs'
   __table_args__ = (
      PrimaryKeyConstraint('cluster_id', 'job_id'),
   )
   cluster_id = Column(String(30), ForeignKey("databricks_clusters.cluster_id"))
   job_id = Column(String(30), ForeignKey("databricks_jobs.job_id"))

class DBricksClusterEvents(Base):
   __tablename__ = 'databricks_cluster_events'
   __table_args__ = (
      PrimaryKeyConstraint('cluster_event_no', 'cluster_id', 'timestamp'),
   )
   cluster_event_no = Column(BigInteger)
   cluster_id = Column(String(30), ForeignKey("databricks_clusters.cluster_id"))
   timestamp = Column(BigInteger)
   timestamp_dt = Column(DateTime)
   cluster_state = Column(String(16))
   cluster_status_flag = Column(Boolean)
   cluster_min_workers = Column(Integer)
   cluster_max_workers = Column(Integer)
   current_num_workers = Column(Integer)
   target_num_workers = Column(Integer)

class DBricksSkuCostPerHour(Base):
   __tablename__ = 'databricks_sku_price'
   __table_args__ = (
      PrimaryKeyConstraint('sku'),
   )
   sku = Column(String(20))
   hourly_price = Column(Float(precision=4))
   version = Column(Integer)
   series = Column(String(20))
   v_cpu = Column(Integer)
   ram_gb = Column(Integer)
   temporary_storage_gb = Column(Integer)
   hourly_price_reserved_1_year = Column(Float(precision=4))
   hourly_price_reserved_3_year = Column(Float(precision=4))
   hourly_price_spot = Column(Float(precision=4))
   os = Column(String(20))
   region = Column(String(20))


Base.metadata.create_all(engine)

