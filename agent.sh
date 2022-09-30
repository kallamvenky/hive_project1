#1 Create a schema based on the given dataset
hive -e 'create table if not exists agentLogingReport(
SLNo int,
Agent string,
Date Timestamp,
LoginTime Timestamp,
LogoutTime Timestamp,
Duration Timestamp
)
row format serde "org.apache.hadoop.hive.serde2.OpenCSVSerde"
with serdeproperties (
    "separatorChar" = ",",
    "quoteChar" = "\"",
"escapeChar" = "\\"
    )
stored as textfile
tblproperties ("skip.header.line.count" = "1")'



hive -e 'create table if not exists agentPerformance(
SLNo int,
Date Timestamp,
AgentName string,
TotalChats int,
AverageResponseTime Timestamp,
AverageResolutionTime Timestamp,
AverageRating int,
TotalFeedback int
)
row format serde "org.apache.hadoop.hive.serde2.OpenCSVSerde"
with serdeproperties (
    "separatorChar" = ",",
    "quoteChar" = "\"",
"escapeChar" = "\\"
    )
stored as textfile
tblproperties ("skip.header.line.count" = "1")'
hive -e 'describe agentLogingReport'
hive -e 'describe agentPerformance'
#2 Dump the data inside the hdfs in the given schema location.
hive -e "load data local inpath 'AgentLogingReport.csv' overwrite into table agentLogingReport"
hive -e "load data local inpath 'AgentPerformance.csv' overwrite into table agentPerformance"
hive -e 'select * from agentLogingReport limit 3'
hive -e 'select * from agentPerformance limit 3'
#3 List of Alla agent names
hive -e 'select distinct AgentName from AgentPerformance'
#4 find out agent avg rating
hive -e 'select AgentName, avg(AverageRating) from AgentPerformance group by AgentName'
#5 total working days of each agent note
hive -e 'select date, Agent from agentLogingReport group by date,Agent'
#6 Total query that each agent have taken
hive -e 'select AgentName, sum(TotalChats) from AgentPerformance group by AgentName'
#7 Total Feedback that each agent have received
hive -e 'select AgentName, sum(TotalFeedback) from AgentPerformance group by AgentName'
#8 Agent name who have average rating between 3.5 to 4
hive -e 'select AgentName, AverageRating from AgentPerformance where AverageRating between 3.5 and 4'
#9 Agent name who have rating less than 3.5
hive -e 'select AgentName, AverageRating from AgentPerformance where AverageRating < 3.5'
#10 Agent name who have rating more than 4.5
hive -e 'select AgentName, AverageRating from AgentPerformance where AverageRating > 4.5'
#11 How many feedback agents have received more than 4.5 average
hive -e 'select count(AverageRating) from AgentPerformance where AverageRating > 4.5'
#12 average weekly response time for each agent ( minutes )
hive -e "with weekresponse as (select AgentName, weekofyear(from_unixtime(unix_timestamp(Date, 'MM/dd/yyyy'),'yyyy-MM-dd')) as week,\
round((hour(AverageResponseTime)*3600+minute(AverageResponseTime)*60+second(AverageResponseTime))/60,2) as responseM \
from AgentPerformance) 
select AgentName, week, avg(responseM) from weekResponse group by AgentName,week" 
#13 average weekly resolution time for each agents
hive -e "with weekresolution as (
select AgentName,\
weekofyear(from_unixtime(unix_timestamp(Date, 'MM/dd/yyyy'),'yyyy-MM-dd')) as week,\
round((hour(AverageResolutionTime)*3600+minute(AverageResolutionTime)*60+second(AverageResolutionTime))/60,2) as resolutionM \
from AgentPerformance)
select AgentName, week, avg(resolutionM) from weekResolution group by AgentName,week"
#14 Find the number of chat on which they have received a feedback
hive -e 'select sum(TotalChats), AgentName from AgentPerformance group by AgentName'
#15 Total contribution hour for each and every agents weekly basis
hive -e "with TotalContribution as (
select Agent,\
weekofyear(from_unixtime(unix_timestamp(Date, 'dd-MMM-yy'),'yyyy-MM-dd')) as week,\
round((hour(Duration)*3600+minute(Duration)*60+second(Duration))/3600,2) as hours \
from AgentLogingReport)
select Agent, week, sum(hours) from TotalContribution group by Agent,week"
#16 Perform inner join, left join and right join based on the agent column and after joining the table 
#export that data into your local system.
#inner join
hive -e "INSERT OVERWRITE LOCAL DIRECTORY 'hp1/inner' \
select * from AgentPerformance ap join AgentLogingReport alr on ap.AgentName=alr.Agent"
#left join
hive -e "INSERT OVERWRITE LOCAL DIRECTORY 'hp1/left' \
select * from AgentPerformance ap left outer join AgentLogingReport alr on ap.AgentName=alr.Agent"
#right join
hive -e "INSERT OVERWRITE LOCAL DIRECTORY 'hp1/right' \
select * from AgentPerformance ap right outer join AgentLogingReport alr on ap.AgentName=alr.Agent"
#17 Perform partitioning on top of the agent column and then on top of that perform bucketing for each partitioning.
#As set variables won't work with hive -e
#So open hive shell by typing hive
#enter the below code inbetween comment and run
: '
SET hive.exec.dynamic.partition=true
set hive.exec.dynamic.partition.mode=nonstrict
SET hive.exec.dynamic.partition.mode=nonstrict
set hive.enforce.bucketing=true

create table if not exists newAgentPerformance(
SLNo int,
Date Timestamp,
TotalChats int,
AverageResponseTime Timestamp,
AverageResolutionTime Timestamp,
AverageRating int,
TotalFeedback int
)
partitioned by (AgentName string)
clustered by (Date)
sorted by (Date)
into 2 buckets
row format serde "org.apache.hadoop.hive.serde2.OpenCSVSerde"
with serdeproperties (
    "separatorChar" = ",",
    "quoteChar" = "\"",
"escapeChar" = "\\"
    )
stored as textfile
tblproperties ("skip.header.line.count" = "1");
INSERT OVERWRITE TABLE newAgentPerformance PARTITION(AgentName) select * from AgentPerformance;
'



