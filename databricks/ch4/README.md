# Cloud Infra - Usage & Cost Monitoring
For Azure DataBricks

## Objectives
#### A. We know how to find out in a DataBricks workspace:
>
1. Details about all DataBricks clusters available (UI / Interactive, Job / Ephemeral) under DataBricks Workspace
> 
2. Details of all jobs scheduled (Name, Schedule, etc.)
> 
3. Correlating information between clusters and jobs, to find out which jobs are run against which clusters
>
4. Jobs run history, run time 

#### B. Push the information into Database (MySQL) tables

#### C. Visualize the data in Apache Superset Dashboards

## Pre-requisites
> Install MySQL
> 
> Install Apache Superset

## Steps

### Step 1: Create required tables 
> clusters -> databricks_clusters
> 
> jobs -> databricks_jobs
> 
> cluster - jobs ->  databricks_cluster_jobs

### Step 2: Set required environment variables
> export DATABRICKS_HOST="https://xyz.azuredatabricks.net"

> export DATABRICKS_TOKEN="dapi............................."

### Step 3: Fetch information from DataBricks, push the data into tables 
> python databricks-clusters.py

> python databricks-jobs.py

> python databricks-e2e-v1-cluster-jobs.py


### References
> https://docs.databricks.com/dev-tools/api/latest/clusters.html 

> https://pypi.org/project/databricks-api/

> https://dev.mysql.com/doc/refman/8.0/en/macos-installation-pkg.html

> https://hub.docker.com/r/apache/superset

> For Apple M1/M2 Processors:
> >docker run -d -p 8080:8088 --name superset --platform linux/amd64 apache/superset
 



