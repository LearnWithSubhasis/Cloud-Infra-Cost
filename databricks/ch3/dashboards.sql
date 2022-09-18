-- cloud-infra-sql
SELECT c.*, j.*
from databricks_clusters c
inner join databricks_cluster_jobs cj on c.cluster_id = cj.cluster_id
inner join databricks_jobs j on cj.job_id = j.job_id;

-- cloud-infra-cluster-events
SELECT c.cluster_name, ce.*
FROM databricks_clusters c inner join
cloud_infra_db.databricks_cluster_events ce
on c.cluster_id = ce.cluster_id;

--






