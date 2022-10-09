-- SKU wise run time and cost
SELECT c.node_type_id,
(sum(jr.job_setup_duration+jr.job_execution_duration) / datediff(max(jr.job_end_time_dt), min(jr.job_start_time_dt))) as avg_daily_runtime_mins,
(sum(sp.hourly_price*(jr.job_setup_duration+jr.job_execution_duration)) / datediff(max(jr.job_end_time_dt), min(jr.job_start_time_dt))) as avg_daily_run_cost,
sum(jr.job_setup_duration+jr.job_execution_duration) as total_runtime_mins,
avg(jr.job_setup_duration+jr.job_execution_duration) as avg_runtime_mins,
sum(sp.hourly_price*(jr.job_setup_duration+jr.job_execution_duration)) as total_cost
FROM cloud_infra_db_us.databricks_jobs_run jr
inner join databricks_clusters c on c.cluster_id = jr.job_cluster_id
inner join databricks_sku_price sp on c.node_type_id = sp.sku
group by c.node_type_id;



