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
 from `rcdatawarehouse.raw.sw_contacts_prod`
), 

--rf jobs transformation
a as (select id, email,source_name, jobs from `rcdatawarehouse.raw.rf_candidate_prod33`),

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


--more flags
lpr as (
select *,
case when stage_name = 'Disqualified' then 1 else 0 end as dq_flag,
1 as sourced_flag,
case when stage_name like '%Hired%' OR stage_name like '%Offer%'OR stage_name like '%client final round%' OR stage_name like '%client second round%' OR stage_name like '%client screen%' OR stage_name like '%intro%' OR stage_name like '%email response%' OR stage like '%Replied%' OR stage like '%Clicked%' OR stage like '%Opened%' OR stage like '%Message%' then 1 else 0 end as messaged_flag,
case when stage_name like '%Hired%' OR stage_name like '%Offer%'OR stage_name like '%client final round%' OR stage_name like '%client second round%' OR stage_name like '%client screen%' OR stage_name like '%intro%' OR stage_name like '%email response%' OR stage like '%Replied%' OR stage like '%Clicked%' OR stage like '%Opened%' then 1 else 0 end as opened_flag,
case when stage_name like '%Hired%' OR stage_name like '%Offer%'OR stage_name like '%client final round%' OR stage_name like '%client second round%' OR stage_name like '%client screen%' OR stage_name like '%intro%' OR stage_name like '%email response%' OR stage like '%Replied%' OR stage like '%Clicked%' then 1 else 0 end as clicked_flag,
case when stage_name like '%Hired%' OR stage_name like '%Offer%'OR stage_name like '%client final round%' OR stage_name like '%client second round%' OR stage_name like '%client screen%' OR stage_name like '%intro%' OR stage_name like '%email response%' OR stage like '%Replied%' then 1 else 0 end as replied_flag,
case when stage_name like '%Hired%' OR stage_name like '%Offer%'OR stage_name like '%client final round%' OR stage_name like '%client second round%' OR stage_name like '%client screen%' OR stage_name like '%intro%' then 1 else 0 end as intro_call_flag,
case when stage_name like '%Hired%' OR stage_name like '%Offer%'OR stage_name like '%client final round%' OR stage_name like '%client second round%' OR stage_name like '%client screen%' OR stage_name like '%submission%' then 1 else 0 end as client_submission_flag,
case when stage_name like '%Hired%' OR stage_name like '%Offer%'OR stage_name like '%client final round%' OR stage_name like '%client second round%' OR stage_name like '%client screen%' then 1 else 0 end as client_screen_flag,
case when stage_name like '%Hired%' OR stage_name like '%Offer%'OR stage_name like '%client final round%' OR stage_name like '%client second round%' then 1 else 0 end as client_second_round_flag, 
case when stage_name like '%Hired%' OR stage_name like '%Offer%'OR stage_name like '%client final round%' then 1 else 0 end as client_final_round_flag, 
case when stage_name like '%Hired%' OR stage_name like '%Offer%'then 1 else 0 end as offer_flag, 
case when stage_name like '%Hired%' then 1 else 0 end as hire_flag
from xer),

-- create real job
rgr as (

select 
lpr.*, 
coalesce(job_name, campaign) as real_job
from lpr
)

--selector and sw funnel
select
*
from rgr
where campaign LIKE '%WH%' OR job_name like '%WH%'
