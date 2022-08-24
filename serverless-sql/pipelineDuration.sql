;WITH ParsedJson AS
(
SELECT TOP 100
    JSON_VALUE (jsonContent, '$.pipelineName') pipelineName
    ,JSON_VALUE (jsonContent, '$.status') AS PipelineStatus
    ,JSON_VALUE(jsonContent,'$.pipelineRunId') pipelineRunId
    ,JSON_VALUE(jsonContent,'$.properties.Output.runStatus.computeAcquisitionDuration') clusterWarmUp
    ,JSON_VALUE(jsonContent,'$.start') startDateTime
    ,JSON_VALUE(jsonContent,'$.end') endDateTime
    ,JSON_VALUE(jsonContent,'$.activityType') activityType
    ,JSON_VALUE(jsonContent,'$.activityRunId') activityRunId
FROM
    OPENROWSET(
        BULK 'https://<your-storage-here>.dfs.core.windows.net/insights-logs-integrationactivityruns/**',
        FORMAT = 'CSV',
        FIELDQUOTE = '0x0b',
        FIELDTERMINATOR ='0x0b'
    )
    WITH (
        jsonContent varchar(MAX)  
    ) AS [result]
),
ActivityRunLevel AS
(
SELECT  *
        ,DATEDIFF(minute,TRY_CAST(startDateTime AS DATETIME2(0)),TRY_CAST(endDateTime AS datetime2(0))) duration 
        /*datediff by sedond results in an error saying the dateparts seperating two instances is too large. Researching a fix as the minute deltas don't look extreme*/
        --,DATEDIFF(second,TRY_CAST(startDateTime AS DATETIME2(0)),TRY_CAST(endDateTime AS datetime2(0))) duration
        ,CASE WHEN clusterWarmUp = 0 THEN 0 ELSE (clusterWarmUp/1000)/60 END clusterWarmUpMin
FROM ParsedJson
WHERE PipelineStatus NOT IN ('Queued','InProgress')

)
SELECT  pipelineName
        ,MIN(startDateTime) pipelineStartTime
        ,MAX(endDateTime) pipelineEndTime
        ,SUM(duration) pipelineDuration
        ,SUM(clusterWarmUpMin) clusterWarmUpMin
        ,SUM(CASE WHEN activityType = 'ExecuteDataFlow' THEN 1 ELSE 0 END) ctDataFlowActivities
FROM ActivityRunLevel
GROUP BY PipelineName
         ,pipelineRunId