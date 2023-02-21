DECLARE Speed_mhz int
DECLARE Speed_ghz decimal(20,2);


SELECT Speed_ghz = CAST(CAST(Speed_mhz AS DECIMAL) / 1000 AS DECIMAL(20,2));


DECLARE CPUcountt INT
DECLARE CPUsocketcount INT
DECLARE CPUHyperthreadratio INT
	SELECT CPUcountt = cpu_count 
	, @CPUsocketcount = [cpu_count] / [hyperthread_ratio]
	, @CPUHyperthreadratio = [hyperthread_ratio]
	FROM sys.dm_os_sys_info;
		
DECLARE BaseWatt MONEY
SELECT BaseWatt = 
CASE 
	WHEN Speed_ghz < 2.2 THEN 55
	WHEN Speed_ghz BETWEEN 2.2 AND 2.5  THEN 50
	WHEN Speed_ghz BETWEEN 2.5 AND 2.8  THEN 45
	WHEN Speed_ghz BETWEEN 2.8 AND 3.2  THEN 40
	WHEN Speed_ghz > 3.2 THEN 35
END

DECLARE WattperCPU MONEY
SELECT WattperCPU = 
CASE 
	WHEN Speed_ghz < 2 THEN 1
	WHEN Speed_ghz BETWEEN 2.0 AND 2.5  THEN 2
	WHEN Speed_ghz BETWEEN 2.5 AND 2.8  THEN 3
	WHEN Speed_ghz BETWEEN 2.8 AND 3.2  THEN 3.5
	WHEN Speed_ghz > 3.2 THEN 4
END

DECLARE TotalWatt MONEY
SELECT TotalWatt = BaseWatt +(WattperCPU *CPUcount)
DECLARE ts BIGINT
SELECT ts =(
	SELECT cpu_ticks/(cpu_ticks/ms_ticks)
	FROM sys.dm_os_sys_info 
	) OPTION (RECOMPILE)
SET STATISTICS IO, TIME ON

DECLARE avgcpu MONEY
	
SELECT avgcpu = AVG(SQLProcessUtilization) 
		FROM 
		(
			SELECT 
			record.value('(./Record/@id)[1]','int') AS record_id
			, record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]','money') AS [SystemIdle]
			, record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]','money') AS [SQLProcessUtilization]
			, [timestamp]
			FROM 
			(
				SELECT
				[timestamp]
				, convert(xml, record) AS [record] 
				FROM sys.dm_os_ring_buffers 
				WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
				AND record LIKE'%%'
			)AS a
		) as b

SELECT 
query_plan
, execution_count
, [AvgCPU(ms)]
, [OverallCPUUsage%] [Average 5 hour CPU %]
, OverallWatt
, 0.5 * OverallWatt [g CO2e]
, CONVERT(MONEY, 0.5 * OverallWatt * 4.8 * 365 /1000) [kg CO2e per year]
-- converted it into a day then to a year and divided it by 1000 to make it a kilogram
FROM (
SELECT TOP 1000
[Plan]
, [AvgCPU(ms)]
, total_worker_time 
,(total_worker_time) *100 / SUM(total_worker_time ) OVER (  PARTITION BY 1.00  ) * avgcpu/100 [OverallCPUUsage%]
,(total_worker_time) *100 / SUM(total_worker_time ) OVER (  PARTITION BY 1.00  ) * avgcpu/100/100 * TotalWatt [OverallWatt]
,[AvgDuration(ms)]
 ,AvgReads
, execution_count
, qp.query_plan
FROM (
SELECT TOP 100/
plan_handle [Plan]
,SUM(total_worker_time)/AVG(execution_count)/1000 AS [AvgCPU(ms)]
,SUM(CONVERT(MONEY,total_worker_time))  total_worker_time
, SUM(total_elapsed_time)/AVG(execution_count)/1000 AS [AvgDuration(ms)]
, (SUM(total_logical_reads)+SUM(total_physical_reads))/AVG(execution_count) AS AvgReads 
, AVG(execution_count ) execution_count
FROM sys.dm_exec_query_stats qs
GROUP BY plan_handle
ORDER BY total_worker_time DESC
) T1
cross apply sys.dm_exec_query_plan([Plan]) qp
ORDER BY total_worker_time DESC
) T2
WHERE query_plan is not NULL