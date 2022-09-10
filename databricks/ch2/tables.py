from sqlalchemy import create_engine, Column, Integer, String, Float, BigInteger, ForeignKey, PrimaryKeyConstraint
from db_session import mysql_session

"""
CREATE DATABASE cloud_infra_db CHARACTER SET utf8;
-- CREATE USER 'cloud_infra_user' IDENTIFIED BY 'cloud_infra_pass';
GRANT ALL PRIVILEGES ON cloud_infra_db.* TO 'cloud_infra_user';
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
   created_time = Column(BigInteger)
   creator_user_name = Column(String(50))
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

Base.metadata.create_all(engine)

