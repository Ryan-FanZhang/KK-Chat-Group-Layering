----Key_Metrics				
select t1.dt,t1.group_num 'TCG',t1.group_num-t3.act_group_num as 'SCG',
t3.act_group_num 'ACG',round(t3.act_group_num/t1.group_num,4) 'ACG/TCG', t4.chat_group_num 'APG',
round(t4.chat_group_num/t3.act_group_num,4) 'APG/TCG',
t3.act_group_num-t4.chat_group_num 'ANPG' from
(select  substr(CAST(ds AS string),1,8) dt,count(distinct groupid ) group_num from beacon_olap.t_updw_ods_kk_group_user_info_base_h
where substr(CAST(ds AS string),1,8) >=cast(substring(from_unixtime(unix_timestamp(now() - interval 30 day), 'yyyyMMdd'),1,8)  AS string)
and is_bot='0'
group by substr(CAST(ds AS string),1,8)) t1
left join 
-----Active_Post_Group (APG)
(select dt,count(distinct groupid) act_group_num from 
(select substr(CAST(ds AS string),1,8) dt ,groupid  from beacon_olap.ieg_gameplus_gameplus_user_action_report_kk
where substr(CAST(ds AS string),1,8)  >=cast(substring(from_unixtime(unix_timestamp(now() - interval 30 day), 'yyyyMMdd'),1,8)  AS string)
and operid in  (
'1102000110101'
)
union all 
select substr(CAST(ds AS string),1,8)  dt ,groupid  from 
beacon_olap.ieg_gameplus_gameplus_noknok_group_im_msg_kk
where substr(CAST(ds AS string),1,8)  >=cast(substring(from_unixtime(unix_timestamp(now() - interval 30 day), 'yyyyMMdd'),1,8)  AS string)
and user_type=1
and uid<>'0'
and opertype in ('1','2','3')) t 
group by dt
 ) t3
on t1.dt=t3.dt 
left join 
----Active_Post_Group (APG)
(select substr(CAST(ds AS string),1,8)  dt ,count(distinct groupid) chat_group_num from 
beacon_olap.ieg_gameplus_gameplus_noknok_group_im_msg_kk
where substr(CAST(ds AS string),1,8)  >=cast(substring(from_unixtime(unix_timestamp(now() - interval 30 day), 'yyyyMMdd'),1,8)  AS string)
and user_type=1
and opertype in ('1','2','3')
and uid<>'0'
group by substr(CAST(ds AS string),1,8) )t4
on t1.dt=t4.dt 
order by  t1.dt desc 