desc access_views.dw_credit_v_credit_bill_keyword_extract;
select dt,bill_month,bill_date,trans_date from access_views.dw_credit_v_credit_bill_keyword_extract where dt='201707' limit 100;
desc fin_access_views.rpd_order;

select * from access_views.dw_credit_v_credit_bill_keyword_extract where dt>='201701' and user_id=186680704 order by dt desc limit 100;

select userid,submit_time,loan_time from fin_access_views.rpd_order where is_loan=1 limit 100;
--加入时间 待确认
create table if not exists vdm_fin.loan_user_analyse2(
    user_id bigint,
    bill_installment bigint,
    min_month string,
    max_month string,
    min_date string,
    max_date string,
    rpd_loan_order_cnt bigint,
    min_submit_time string,
    max_submit_time string);
set mapreduce.job.queuename=root.fin_data_dev;
insert overwrite table vdm_fin.loan_user_analyse2
select a.*, b.rpd_loan_order_cnt,b.min_submit_time,b.max_submit_time
from (select user_id,sum(is_bill_installment) as bill_installment,
        min(dt) as min_month,max(dt) as max_month,
        min(trans_date) as min_date,max(trans_date) as max_date
        from access_views.dw_credit_v_credit_bill_keyword_extract 
        where dt<='201707' and dt>='201608'
        group by user_id) a
left join (select cast(userid as bigint) as user_id,count(1) as rpd_loan_order_cnt,
            min(submit_time) as min_submit_time,max(submit_time) as max_submit_time
            from fin_access_views.rpd_order
            where is_loan=1
            group by userid) b
on a.user_id=b.user_id;

select count(*) from vdm_fin.loan_user_analyse2;
select * from vdm_fin.loan_user_analyse2 where rpd_loan_order_cnt>=1 limit 100;

--每周贷款用户趋势
select c.*,d.new_user,c.day_user-d.new_user as old_user
from
    (select a.dt,sum(a.amount) as loan_amount,count(1) as day_user
    from 
        (select substr(submit_time,1,10) as dt,amount
        from fin_access_views.rpd_order 
        where is_loan=1) a
    group by a.dt)c
left join
    (select b.first_loan,count(1) as new_user
    from 
        (select userid,substr(min(submit_time),1,10) as first_loan 
        from fin_access_views.rpd_order 
        where is_loan=1 group by userid) b
    group by b.first_loan) d
on c.dt=d.first_loan;

--1--用户app分布
create table if not exists vdm_fin.user_apps_distribution(
    user_id string,
    app_num bigint,
    fin_app_num bigint,
    app_names string,
    rpd_loan_order_cnt bigint,
    rpd_loan_order_amt bigint);
insert overwrite table vdm_fin.user_apps_distribution
select a.*,b.rpd_loan_order_cnt,b.rpd_loan_order_amt
from 
    (select cast(userid as string) as user_id,
        count(1) as app_num,
        sum(case when type='理财' then 1 else 0 end)as fin_app_num,
        concat_ws(';',collect_set(case when type='理财' then app_name else null end)) as app_names
    from 
        (select distinct userid,app_name,type 
        from fin_access_views.activeuser_pkg_name)t 
        group by userid) a
left join
        (select user_id,rpd_loan_order_cnt,rpd_loan_order_amt
        from fin_access_views.dm_fin_t_customer_dna_operator
        where dt='2017-07-31') b
on a.user_id=b.user_id;

select fin_app_num,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.user_apps_distribution
where user_id is not null
group by fin_app_num order by fin_app_num;

--账单记录（每月）统计
select dt,count(*) as rec_num,count(distinct user_id) as user_num  
from access_views.dw_credit_v_credit_bill_keyword_extract 
where dt>='201601' and dt<='201707' 
group by dt;

--用户画像
select rpd_loan_order_cnt,count(*) 
from fin_access_views.dm_fin_t_customer_dna_operator
where dt='2017-07-25'
group by rpd_loan_order_cnt;

--账单记录表
select amount_money,discription 
from access_views.dw_credit_v_credit_bill_keyword_extract 
where dt='201707' and is_travel=1 limit 300;

select amount_money,discription 
from access_views.dw_credit_v_credit_bill_keyword_extract 
where dt='201707' and discription like '%分期%' limit 300;


--账单明细提单分布
select count(1) as cnt,
    sum(case when a.rpd_order_1w>=1 then 1 else 0 end) as rpd_order_1w,
    sum(case when a.rpd_order_1w>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_1w_rate,
    sum(case when a.rpd_order_1m>=1 then 1 else 0 end) as rpd_order_1m,
    sum(case when a.rpd_order_1m>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_1m_rate,
    sum(case when a.rpd_order_3m>=1 then 1 else 0 end) as rpd_order_3m,
    sum(case when a.rpd_order_3m>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_3m_rate,
    sum(case when a.rpd_order_6m>=1 then 1 else 0 end) as rpd_order_6m,
    sum(case when a.rpd_order_6m>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_6m_rate,
    sum(case when a.rpd_order_1y>=1 then 1 else 0 end) as rpd_order_1y,
    sum(case when a.rpd_order_1y>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_1y_rate
from 
    (select cast(rpd_loan_order_cnt>=1 as int) as is_loan,
        rpd_order_1w,rpd_order_1m,rpd_order_3m,rpd_order_6m,rpd_order_1y
    from vdm_fin.loan_user_analyse) a
group by a.is_loan;
--用户画像提单分布
select 'customer_dna',0,count(1),
    sum(case when rpd_order_1w>=1 then 1 else 0 end) as rpd_order_1w,
    sum(case when rpd_order_1w>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_1w_rate,
    sum(case when rpd_order_1m>=1 then 1 else 0 end) as rpd_order_1m,
    sum(case when rpd_order_1m>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_1m_rate,
    sum(case when rpd_order_3m>=1 then 1 else 0 end) as rpd_order_3m,
    sum(case when rpd_order_3m>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_3m_rate,
    sum(case when rpd_order_6m>=1 then 1 else 0 end) as rpd_order_6m,
    sum(case when rpd_order_6m>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_6m_rate,
    sum(case when rpd_order_1y>=1 then 1 else 0 end) as rpd_order_1y,
    sum(case when rpd_order_1y>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_1y_rate
from fin_access_views.dm_fin_t_customer_dna_operator where dt='2017-07-31'and rpd_loan_order_cnt=0
union all 
select 'customer_dna',1,count(1),
    sum(case when rpd_order_1w>=1 then 1 else 0 end) as rpd_order_1w,
    sum(case when rpd_order_1w>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_1w_rate,
    sum(case when rpd_order_1m>=1 then 1 else 0 end) as rpd_order_1m,
    sum(case when rpd_order_1m>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_1m_rate,
    sum(case when rpd_order_3m>=1 then 1 else 0 end) as rpd_order_3m,
    sum(case when rpd_order_3m>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_3m_rate,
    sum(case when rpd_order_6m>=1 then 1 else 0 end) as rpd_order_6m,
    sum(case when rpd_order_6m>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_6m_rate,
    sum(case when rpd_order_1y>=1 then 1 else 0 end) as rpd_order_1y,
    sum(case when rpd_order_1y>=1 then 1 else 0 end)*1.0/count(1) as rpd_order_1y_rate
from fin_access_views.dm_fin_t_customer_dna_operator where dt='2017-07-31'and rpd_loan_order_cnt>=1;


--人品贷贷款用户数：677339
select count(*) 
from fin_access_views.dm_fin_t_customer_dna_operator
where dt='2017-07-31' and rpd_loan_order_cnt>=1;
--人品贷再贷用户数：210114
select count(*) 
from fin_access_views.dm_fin_t_customer_dna_operator
where dt='2017-07-31' and rpd_loan_order_cnt>=1;
--人品贷贷款用户数2017年导入账单人数：640121
select count(*) from vdm_fin.loan_user_analyse where rpd_loan_order_cnt>=1;
--人品贷贷款用户数2017年导入账单人数且含有分期字段:527420
select count(*) from vdm_fin.loan_user_analyse where rpd_loan_order_cnt>=1 and bill_installment>=1;

--资金需求
--类别分布
create table if not exists vdm_fin.bill_month_analyse(
    month string,
    label string,
    value int,
    cnt int);
 
insert into vdm_fin.bill_month_analyse
select dt as month,'is_car' as label,is_car as value, count(distinct user_id) as cnt 
from access_views.dw_credit_v_credit_bill_keyword_extract 
where dt>='201701' and dt<='201706' group by dt,is_car;

insert into vdm_fin.bill_month_analyse
select dt as month,'is_high_level_car' as label,is_high_level_car as value, count(distinct user_id) as cnt 
from access_views.dw_credit_v_credit_bill_keyword_extract 
where dt>='201701' and dt<='201706' group by dt,is_high_level_car;

insert into vdm_fin.bill_month_analyse
select dt as month,'is_house' as label,is_house as value, count(distinct user_id) as cnt 
from access_views.dw_credit_v_credit_bill_keyword_extract 
where dt>='201701' and dt<='201706' group by dt,is_house;

insert into vdm_fin.bill_month_analyse
select dt as month,'is_travel' as label,is_travel as value, count(distinct user_id) as cnt 
from access_views.dw_credit_v_credit_bill_keyword_extract 
where dt>='201701' and dt<='201706' group by dt,is_travel;

--总体情况
create table if not exists vdm_fin.bill_month_distribution(
    month string,
    tolal bigint,
    appear bigint);
insert into vdm_fin.bill_month_distribution
select dt as month,count(*) as total,count(distinct user_id) as appear 
from access_views.dw_credit_v_credit_bill_keyword_extract 
where dt>='201701' and dt<='201706' group by dt;

--关联
select e.* ,t.tolal,t.appear,e.n1*1.0/t.appear as n1_ratio
from (select c.month,c.label,c.n0,d.n1
        from (select a.month,a.label,a.cnt as n0 from vdm_fin.bill_month_analyse a where a.value=0) c
        left join (select b.month,b.label,b.cnt as n1 from vdm_fin.bill_month_analyse b where b.value=1) d
        on c.month=d.month and c.label=d.label) e
left join vdm_fin.bill_month_distribution t 
on e.month=t.month;

--日导入账单人数分布
select import_date,count(*) as cnt,count(distinct user_id) as uid_cnt 
from dw_access_views.dws_debt_v_agg_import_bill where dt_month='2017-08' 
group by import_date order by import_date limit 100;

--2--用户 消费字段 分布
drop table vdm_fin.loan_user_analyse;
create table vdm_fin.loan_user_analyse as 
select a.*, b.rpd_loan_order_cnt,b.rpd_order_1w,b.rpd_order_1m,b.rpd_order_3m,b.rpd_order_6m,b.rpd_order_1y
from 
    (select user_id,
        sum(is_travel) as travel,
        sum(is_outting) as outting,
        sum(is_hotel) as hotel,
        sum(is_high_level_hotel) as high_level_hotel,
        sum(is_common_hotel) as common_hotel,
        sum(is_restaurant) as restaurant,
        sum(is_high_level_restaurant) as high_level_restaurant,
        sum(is_common_restaurant) as common_restaurant,
        sum(is_cinema) as cinema,
        sum(is_entertainment) as entertainment,
        sum(is_health_care) as health_care,
        sum(is_dressing) as dressing,
        sum(is_luxuries) as luxuries,
        sum(is_clothing) as clothing,
        sum(is_brand_clothing) as brand_clothing,
        sum(is_common_clothing) as common_clothing,
        sum(is_group_buying) as group_buying,
        sum(is_hobby) as hobby,
        sum(is_education) as education,
        sum(is_pet) as pet,
        sum(is_medical) as medical,
        sum(is_logistics) as logistics,
        sum(is_wedding) as wedding,
        sum(is_mother_baby) as mother_baby,
        sum(is_supermarket) as supermarket,
        sum(is_online_shopping) as online_shopping,
        sum(is_thirdparty_pay) as thirdparty_pay,
        sum(is_bill_installment) as bill_installment,
        sum(is_car) as car,
        sum(is_high_level_car) as high_level_car,
        sum(is_home_living) as home_living,
        sum(is_3c_consume) as q3c_consume,
        sum(is_house) as house,
        sum(is_lottery) as lottery,
        sum(is_insurance) as insurance,
        sum(is_loan) as loan,
        sum(is_investment) as investment,
        sum(is_bank_business) as bank_business,
        sum(is_trade) as trade,
        sum(is_food) as food,
        sum(is_rent_house) as rent_house,
        sum(is_home_decoration) as home_decoration,
        sum(is_housekeeping) as housekeeping,
        sum(is_literary) as literary,
        sum(is_oversea) as oversea
    from access_views.dw_credit_v_credit_bill_keyword_extract 
    where dt<='201707' and dt>='201701'
    group by user_id) a
left join 
    (select cast(user_id as bigint) as user_id,rpd_loan_order_cnt,rpd_order_1w,rpd_order_1m,rpd_order_3m,rpd_order_6m,rpd_order_1y
    from fin_access_views.dm_fin_t_customer_dna_operator
    where dt='2017-07-31') b
on a.user_id=b.user_id;

--插入
create table if not exists vdm_fin.lcl_loan_user_result(
    type string,
    value bigint,
    user_num bigint,
    user_loan_num bigint,
    loan_rate double,
    reloan_rate double,
    num1 bigint,
    p1 double,
    num2 bigint,
    p2 double);
insert overwrite table vdm_fin.lcl_loan_user_result
select 'travel' as type,travel as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by travel
union all
select 'outting' as type,outting as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by outting
union all
select 'hotel' as type,hotel as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by hotel
union all
select 'high_level_hotel' as type,high_level_hotel as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by high_level_hotel
union all
select 'common_hotel' as type,common_hotel as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by common_hotel
union all
select 'restaurant' as type,restaurant as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by restaurant
union all
select 'high_level_restaurant' as type,high_level_restaurant as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by high_level_restaurant
union all
select 'common_restaurant' as type,common_restaurant as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by common_restaurant
union all
select 'cinema' as type,cinema as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by cinema
union all
select 'entertainment' as type,entertainment as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by entertainment
union all
select 'health_care' as type,health_care as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by health_care
union all
select 'dressing' as type,dressing as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by dressing
union all
select 'luxuries' as type,luxuries as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by luxuries
union all
select 'clothing' as type,clothing as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by clothing
union all
select 'brand_clothing' as type,brand_clothing as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by brand_clothing
union all
select 'common_clothing' as type,common_clothing as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by common_clothing
union all
select 'group_buying' as type,group_buying as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by group_buying
union all
select 'hobby' as type,hobby as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by hobby
union all
select 'education' as type,education as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by education
union all
select 'pet' as type,pet as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by pet
union all
select 'medical' as type,medical as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by medical
union all
select 'logistics' as type,logistics as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by logistics
union all
select 'wedding' as type,wedding as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by wedding
union all
select 'mother_baby' as type,mother_baby as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by mother_baby
union all
select 'supermarket' as type,supermarket as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by supermarket
union all
select 'online_shopping' as type,online_shopping as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by online_shopping
union all
select 'thirdparty_pay' as type,thirdparty_pay as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by thirdparty_pay
union all
select 'bill_installment' as type,bill_installment as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by bill_installment
union all
select 'car' as type,car as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by car
union all
select 'high_level_car' as type,high_level_car as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by high_level_car
union all
select 'home_living' as type,home_living as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by home_living
union all
select 'q3c_consume' as type,q3c_consume as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by q3c_consume
union all
select 'house' as type,house as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by house
union all
select 'lottery' as type,lottery as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by lottery
union all
select 'insurance' as type,insurance as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by insurance
union all
select 'loan' as type,loan as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by loan
union all
select 'investment' as type,investment as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by investment
union all
select 'bank_business' as type,bank_business as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by bank_business
union all
select 'trade' as type,trade as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by trade
union all
select 'food' as type,food as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by food
union all
select 'rent_house' as type,rent_house as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by rent_house
union all
select 'home_decoration' as type,home_decoration as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by home_decoration
union all
select 'housekeeping' as type,housekeeping as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by housekeeping
union all
select 'literary' as type,literary as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by literary
union all
select 'oversea' as type,oversea as value,count(1) as user_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,
    sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,
    sum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,
    sum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2
from vdm_fin.loan_user_analyse
where user_id is not null
group by oversea;

--取数
select type,count(1) as value_num,
    sum(loan_rate)/count(1) as mean_laon_rate,
    sum(reloan_rate)/count(1) as mean_relaon_rate
from vdm_fin.lcl_loan_user_result 
group by type
order by mean_laon_rate desc;



