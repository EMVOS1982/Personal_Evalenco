select ht.user_name, ht.transaction_id,ht.operation,ht.query, ht.created_at
from "uat-rds-audit"."db_aba_connect_backend" dbcn
inner join "uat-rds-audit"."history_tracking" ht on ht.transaction_id = dbcn.virtual_transaction_id and
ht.query = dbcn.query and ht.operation =  dbcn.command_tag



select *--replace(au.message,'"','') as uno, replace(ht.query,'"','') as dos
from "uat-rds-audit"."history_tracking" ht
left join "uat-rds-audit"."db_aba_connect_backend" au on au.virtual_transaction_id = ht.transaction_id
where ht.transaction_id = '6/286244' and ht.operation = au.command_tag and replace(au.message,'"','') like concat('%',replace(ht.query,'"',''),'%')


select count (1), command_tag 
from "uat-rds-audit"."db_aba_connect_backend" 
where date(cast(log_time as timestamp)) = date('2024-01-16') 
group by command_tag order by count (1) desc


select *--replace(au.message,'"','') as uno, replace(ht.query,'"','') as dos
from "uat-rds-audit"."history_tracking" ht
left join "uat-rds-audit"."db_aba_connect_backend" au on au.virtual_transaction_id = ht.transaction_id
where  date(cast(au.log_time as timestamp)) = date('2024-01-16') and ht.operation = au.command_tag and replace(au.message,'"','') like concat('%',replace(ht.query,'"',''),'%')


with prueba as (select ROW_NUMBER() OVER (PARTITION BY ht.transaction_id) rn, *--replace(au.message,'"','') as uno, replace(ht.query,'"','') as dos
from "uat-rds-audit"."history_tracking" ht
left join "uat-rds-audit"."db_aba_connect_backend" au on au.virtual_transaction_id = ht.transaction_id
where  date(cast(au.log_time as timestamp)) = date('2024-01-16') and ht.operation = au.command_tag and replace(au.message,'"','') like concat('%',replace(ht.query,'"',''),'%') )
select * from prueba where rn = 1


with data as (select * from db_aba_connect_backend where command_tag = 'BIND' limit 20)
select
--REGEXP_EXTRACT(message, 'client_id =(\d+)') AS client_id,
* from data


SELECT process_id, virtual_transaction_id,message,command_tag
from "db_aba_connect_backend"  
where command_tag in ('SELECT','BIND')
and message like '%_versions%'
and message not like '%TABLE,pg_%'
and  date(cast(log_time as timestamp)) >= date('2024-09-18')


SELECT *
FROM history_tracking
where  "$path" = 's3://prod-rds-audit-log/icbdproduction1/ht_icbd_production/20250214.txt'
limit 1;





