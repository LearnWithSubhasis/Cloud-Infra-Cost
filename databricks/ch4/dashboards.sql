-- cloud-infra-sql-ch4
SELECT c.*, j.*
from databricks_clusters c
inner join databricks_cluster_jobs cj on c.cluster_id = cj.cluster_id
inner join databricks_jobs j on cj.job_id = j.job_id;

-- cloud-infra-cluster-events-ch4
SELECT c.cluster_name, ce.*
FROM databricks_clusters c
inner join databricks_cluster_events ce
on c.cluster_id = ce.cluster_id;

-- cluster-jobs-run-ch4
SELECT c.cluster_id, c.cluster_name, j.job_name, j.pause_status, j.schedule_day_of_week, j.schedule_hour_of_day, j.schedule_min_of_hour, jr.*
from databricks_clusters c
inner join databricks_cluster_jobs cj on c.cluster_id = cj.cluster_id
inner join databricks_jobs j on cj.job_id = j.job_id
inner join databricks_jobs_run jr on j.job_id = jr.job_id

-- cloud-cost
SELECT c.node_type_id,
(sum(jr.job_setup_duration+jr.job_execution_duration) / datediff(max(jr.job_end_time_dt), min(jr.job_start_time_dt))) as avg_daily_runtime_mins,
(sum(sp.hourly_price*(jr.job_setup_duration+jr.job_execution_duration)) / datediff(max(jr.job_end_time_dt), min(jr.job_start_time_dt))) as avg_daily_run_cost,
sum(jr.job_setup_duration+jr.job_execution_duration) as total_runtime_mins,
avg(jr.job_setup_duration+jr.job_execution_duration) as avg_runtime_mins,
sum(sp.hourly_price*(jr.job_setup_duration+jr.job_execution_duration)) as total_cost
FROM cloud_infra_db_us.databricks_jobs_run jr
inner join databricks_clusters c on c.cluster_id = jr.job_cluster_id
inner join databricks_sku_price sp on c.node_type_id = sp.sku
group by c.node_type_id




