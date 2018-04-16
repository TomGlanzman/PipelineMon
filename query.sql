
WITH 
LatestTasks AS (
    SELECT a.*
    FROM Task a
        INNER JOIN (
            SELECT version, taskname, MAX(revision) revision
            FROM Task
            GROUP BY version, taskname
        ) b ON a.version = b.version AND a.revision = b.revision and a.taskname = b.taskname
        INNER JOIN (
            SELECT taskname, MAX(version) version
            FROM Task
            GROUP BY taskname
        ) c ON a.version = c.version and a.taskname = c.taskname
    WHERE a.parenttask = 0
),
Tasks (task, taskname) AS (
    -- Recursive CTE query
    SELECT task, taskname FROM LatestTasks WHERE taskname = :task_name
    UNION ALL
    SELECT t.task, t.taskname
      FROM TASK t
      JOIN Tasks p on (p.task = t.parenttask)
)
SELECT t.*, p.processname, p.process, p.autoretrymaxattempts, s.rootstream, root.streamid rootstreamid, s.executionnumber streamexecutionnumber,
s.islatest isLatestStream, s.stream, s.streamid, pi.*, bpi.*
   FROM Tasks t
   JOIN Process p on (t.task = p.task)
   JOIN ProcessInstance pi on (p.process = pi.process)
   JOIN Stream s on (pi.stream = s.stream)
   JOIN Stream root on (s.rootstream = root.stream)
   JOIN BatchProcessInstance bpi on (pi.processinstance = bpi.processinstance)
   order by s.rootstream, t.task, s.streamid, p.process, pi.executionnumber, pi.autoretrynumber;



