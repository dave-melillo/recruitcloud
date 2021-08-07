--source whale load
with sw1 as (
select 
*, 
REPLACE(
  REPLACE(
    REPLACE(
      emails,
      "'",
      ''),
    '[',
    ''),
  ']',
  '') as sw_clean_email,
 from `rcdatawarehouse.raw.sw_contacts1`
), 

--rf jobs transformation
a as (select id, email,source_name, jobs from `rcdatawarehouse.raw.rf_candidate2`),

b as (select id, email,source_name, REPLACE (jobs, "'", "\"") as jobs from a),

c as (select id, email,source_name, REPLACE (jobs, "None", "\"None\"") as jobs from b),

d as (select id, email,source_name, REPLACE (jobs, "True", "\"True\"") as jobs from c),

--rf email transformation and jobs json unnesting
rf as (
select 
id,
REPLACE(
  REPLACE(
    REPLACE(
      email,
      "'",
      ''),
    '[',
    ''),
  ']',
  '') as rf_clean_email,
source_name,
JSON_EXTRACT_SCALAR(json, '$.client_company_name') AS client_company_name,  
JSON_EXTRACT_SCALAR(json, '$.stage_name') AS stage_name,  
JSON_EXTRACT_SCALAR(json, '$.stage_moved') AS stage_moved,
JSON_EXTRACT_SCALAR(json, '$.name') AS job_name,
JSON_EXTRACT_SCALAR(json, '$.disqualification_reason') AS disqualification_reason

from d, UNNEST(JSON_EXTRACT_ARRAY(jobs , '$')) json),

--rf unnesting emails
ik as (
select 
rf.*, xre
from rf,
UNNEST(SPLIT(rf_clean_email, ",")) xre 
),

ek as (
select 
*, 
ROW_NUMBER() over (PARTITION BY xre ORDER BY stage_moved desc) as rn
from ik
),

nk as (select distinct * from ek where xre != '' and rn = 1),

--join source whale and recruiter flow
bt as (
select distinct
sw1.*,
'break' as breaktag, 
nk.*
from sw1
left join nk on sw1.sw_clean_email  = nk.xre
where sw_clean_email != ''
),

--source and stage cleaner
xer as (
select 

bt.*, 
case 
when lower(source_name) like '%signal%' then 'signalhire'
when lower(source_name) like '%linkedin%' then 'linkedin'
when lower(source_name) like '%human%' then 'humanpredictions'
when lower(source_name) like '%seek%' then 'seekout'
when lower(source_name) like '%hiretual%' then 'hiretual'
when lower(source_name) like '%spread%' then 'spreadsheet'
else 'other'
end as source_clean,

lower(stage_name) as stage_name_cleaner


from bt),

--create flags
fer as (
select 
*,
case when stage_name = 'Sourced' then 1 else 0 end as source_flag,
case when stage_name = 'Disqualified' then 1 else 0 end as dq_flag,
case when stage_name = 'contacted LinkedIn' then 1 else 0 end as contacted_flag,
case when stage_name  IN('email response','responded LinkedIn') then 1 else 0 end as response_flag,
case when stage_name IN('Client Submission','Applied') then 1 else 0 end as applied_flag,
case when stage_name = 'Hired' then 1 else 0 end as hired_flag
from xer ),

--more flags
lpr as (
select *,
CASE WHEN hired_flag = 1 OR applied_flag = 1 OR response_flag = 1 OR contacted_flag = 1 OR source_flag = 1 then 1 else 0 end as source_flag_real, 
CASE WHEN hired_flag = 1 OR applied_flag = 1 OR response_flag = 1 OR contacted_flag = 1  then 1 else 0 end as contacted_flag_real, 
CASE WHEN hired_flag = 1 OR applied_flag = 1 OR response_flag = 1 then 1 else 0 end as response_flag_real, 
CASE WHEN hired_flag = 1 OR applied_flag = 1 then 1 else 0 end as applied_flag_real,
CASE WHEN hired_flag = 1 then 1 else 0 end as hired_flag_real,
from fer),

-- create real job
rgr as (

select 
lpr.*, 
coalesce(job_name, campaign) as real_job
from lpr
)

--selector and sw funnel
select
*,
1 as sent_flag, 
case when stage IN ('Replied') OR clicks > 0 then 1 else 0 end as clicked_flag, 
case when stage IN ('Replied') OR opens > 0 then 1 else 0 end as opened_flag, 
case when stage IN ('Replied') then 1 else 0 end as reply_flag
from rgr
where campaign LIKE '%WH%' OR job_name like '%WH%'
