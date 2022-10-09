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

-- cluster-jobs-run
SELECT c.cluster_id, c.cluster_name, j.job_name, j.pause_status, j.schedule_day_of_week, j.schedule_hour_of_day, j.schedule_min_of_hour, jr.*
from databricks_clusters c
inner join databricks_cluster_jobs cj on c.cluster_id = cj.cluster_id
inner join databricks_jobs j on cj.job_id = j.job_id
inner join databricks_jobs_run jr on j.job_id = jr.job_id

-- jobs-run-tenant1
SELECT c.cluster_id, c.cluster_name, c.node_type_id, c.min_workers, c.max_workers, j.job_name, j.pause_status, j.schedule_day_of_week, j.schedule_hour_of_day, j.schedule_min_of_hour, jr.*
from databricks_clusters c
inner join databricks_jobs_run jr on c.cluster_id = jr.job_cluster_id
inner join databricks_jobs j on j.job_id = jr.job_id

-- cluster-events-tenant1
SELECT c.cluster_name, c.node_type_id, c.autotermination_minutes, c.cluster_source, c.min_workers, c.max_workers, ce.*
FROM databricks_cluster_events ce
left join databricks_clusters c on c.cluster_id = ce.cluster_id

-- cluster-jobs-tenant1
SELECT c.*, j.*
from databricks_clusters c
--inner join databricks_cluster_jobs cj on c.cluster_id = cj.cluster_id
inner join databricks_jobs j on c.cluster_id = j.existing_cluster_id;

-- cluster-events-tenant2
SELECT c.cluster_name, c.node_type_id, c.autotermination_minutes, c.cluster_source, c.min_workers, c.max_workers, ce.*
FROM databricks_cluster_events ce
left join databricks_clusters c on c.cluster_id = ce.cluster_id

-- jobs-run-tenant2
SELECT c.cluster_id, c.cluster_name, c.cluster_source, c.node_type_id, c.min_workers, c.max_workers, ce.current_num_workers, j.job_name, j.pause_status, j.schedule_day_of_week, j.schedule_hour_of_day, j.schedule_min_of_hour, jr.*
from databricks_clusters c
inner join databricks_jobs_run jr on c.cluster_id = jr.job_cluster_id
inner join databricks_jobs j on j.job_id = jr.job_id
inner join databricks_cluster_events ce on ce.cluster_id = c.cluster_id

-- cluster-jobs-tenant2
SELECT c.*, j.*
from databricks_clusters c
--inner join databricks_cluster_jobs cj on c.cluster_id = cj.cluster_id
inner join databricks_jobs j on c.cluster_id = j.existing_cluster_id;



-- SKU wise usage and cost
SELECT c.node_type_id,
(sum(jr.job_setup_duration+jr.job_execution_duration) / datediff(max(jr.job_end_time_dt), min(jr.job_start_time_dt))) as avg_daily_runtime_mins,
(sum(sp.hourly_price*(jr.job_setup_duration+jr.job_execution_duration)) / datediff(max(jr.job_end_time_dt), min(jr.job_start_time_dt))) as avg_daily_run_cost,
sum(jr.job_setup_duration+jr.job_execution_duration) as total_runtime_mins,
avg(jr.job_setup_duration+jr.job_execution_duration) as avg_runtime_mins,
sum(sp.hourly_price*(jr.job_setup_duration+jr.job_execution_duration)) as total_cost
FROM databricks_jobs_run jr
inner join databricks_clusters c on c.cluster_id = jr.job_cluster_id
inner join databricks_sku_price sp on c.node_type_id = sp.sku
group by c.node_type_id;


-- 15 mins aggregation analysis
SELECT @begin_datetime := date(min(job_start_time_dt)) from databricks_jobs_run;
SELECT @end_datetime := max(job_end_time_dt) from databricks_jobs_run;
SET @interval_mins = 15;

SELECT jr.job_run_name, c.node_type_id,
    jr.job_start_time_dt,
    TIMESTAMPDIFF(MINUTE, @begin_datetime, jr.job_start_time_dt) AS diff,
    CEILING(TIMESTAMPDIFF(MINUTE, @begin_datetime, jr.job_start_time_dt) / @interval_mins) * @interval_mins AS diff_round_up,
    DATE_ADD(@begin_datetime, Interval CEILING(TIMESTAMPDIFF(MINUTE, @begin_datetime, jr.job_start_time_dt) / @interval_mins) * @interval_mins minute) AS job_start_datetime_15_min_interval,
	c.max_workers as max_workers
FROM databricks_jobs_run jr
inner join databricks_clusters c on c.cluster_id = jr.job_cluster_id
inner join databricks_sku_price sp on c.node_type_id = sp.sku
ORDER BY job_start_time_dt;

SELECT node_type_id, job_start_datetime_15_min_interval,
max(max_workers) as max_workers, sum(max_workers) as total_workers, CAST(avg(max_workers) as UNSIGNED) as avg_workers from
(
	SELECT jr.job_run_name, c.node_type_id,
		jr.job_start_time_dt,
		TIMESTAMPDIFF(MINUTE, @begin_datetime, jr.job_start_time_dt) AS diff,
		CEILING(TIMESTAMPDIFF(MINUTE, @begin_datetime, jr.job_start_time_dt) / @interval_mins) * @interval_mins AS diff_round_up,
		DATE_ADD(@begin_datetime, Interval CEILING(TIMESTAMPDIFF(MINUTE, @begin_datetime, jr.job_start_time_dt) / @interval_mins) * @interval_mins minute) AS job_start_datetime_15_min_interval,
		c.max_workers as max_workers
	FROM databricks_jobs_run jr
	inner join databricks_clusters c on c.cluster_id = jr.job_cluster_id
	inner join databricks_sku_price sp on c.node_type_id = sp.sku
	ORDER BY job_start_time_dt
) 15_mins_job_runs
group by node_type_id, job_start_datetime_15_min_interval
order by job_start_datetime_15_min_interval;


SELECT node_type_id, max(max_workers) as max_of_max_workers,
max(avg_workers) as max_of_avg_workers, max(total_workers) as max_of_total_workers FROM
(
    SELECT node_type_id, job_start_datetime_15_min_interval,
    max(max_workers) as max_workers, sum(max_workers) as total_workers, CAST(avg(max_workers) as UNSIGNED) as avg_workers from
    (
        SELECT jr.job_run_name, c.node_type_id,
            jr.job_start_time_dt,
            TIMESTAMPDIFF(MINUTE, @begin_datetime, jr.job_start_time_dt) AS diff,
            CEILING(TIMESTAMPDIFF(MINUTE, @begin_datetime, jr.job_start_time_dt) / @interval_mins) * @interval_mins AS diff_round_up,
            DATE_ADD(@begin_datetime, Interval CEILING(TIMESTAMPDIFF(MINUTE, @begin_datetime, jr.job_start_time_dt) / @interval_mins) * @interval_mins minute) AS job_start_datetime_15_min_interval,
            c.max_workers as max_workers
        FROM databricks_jobs_run jr
        inner join databricks_clusters c on c.cluster_id = jr.job_cluster_id
        inner join databricks_sku_price sp on c.node_type_id = sp.sku
        ORDER BY job_start_time_dt
    ) 15_mins_job_runs
    group by node_type_id, job_start_datetime_15_min_interval
    order by job_start_datetime_15_min_interval
) 15_mins_job_runs_summary
group by node_type_id;







