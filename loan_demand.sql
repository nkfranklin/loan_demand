create table vdm_fin.lcl_loan_demand as
select a.*,
    t.label,
    b.travel,b.outting,b.hotel,b.high_level_hotel,b.common_hotel,b.restaurant,b.high_level_restaurant,b.common_restaurant,b.cinema,b.entertainment,b.health_care,b.dressing,b.luxuries,b.clothing,b.brand_clothing,b.common_clothing,b.group_buying,b.hobby,b.education,b.pet,b.medical,b.logistics,b.wedding,b.mother_baby,b.supermarket,b.online_shopping,b.thirdparty_pay,b.bill_installment,b.car,b.high_level_car,b.home_living,b.q3c_consume,b.house,b.lottery,b.insurance,b.loan,b.investment,b.bank_business,b.trade,b.food,b.rent_house,b.home_decoration,b.housekeeping,b.literary,b.oversea,
    c.app_num,
    c.fin_app_num,
    c.fin_app_rate,
    c.app_names
from
    (select *
    from fin_access_views.dm_fin_t_customer_dna_operator
    where dt='2017-08-17') a
left join
    (select user_id,rpd_order_1w as label
    from fin_access_views.dm_fin_t_customer_dna_operator
    where dt='2017-08-24') t
on a.user_id=t.user_id
left join 
    (select cast(user_id as string) as user_id,
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
    where dt>='201705'
    group by user_id) b
on a.user_id=b.user_id
left join 
    (select cast(userid as string) as user_id,
        count(1) as app_num,
        sum(case when type='理财' then 1 else 0 end)as fin_app_num,
        sum(case when type='理财' then 1 else 0 end)*1.0/count(1) as fin_app_rate,
        concat_ws(';',collect_set(case when type='理财' then app_name else null end)) as app_names
    from 
        (select distinct userid,app_name,type 
        from fin_access_views.activeuser_pkg_name)t 
    group by userid) c 
on a.user_id=c.user_id;


--05-17
select sum(case when rpd_homepage_1w>=1 then 1 else 0 end),--一周活跃1050444
    sum(case when rpd_homepage_1w>=1 and rpd_loan_order_cnt>=1 then 1 else 0 end),--一周活跃贷款用户 
    sum(case when rpd_homepage_1w>=1 and label>=1 then 1 else 0 end),--一周活跃下周下单105875,0.1007
    sum(case when travel is not null then 1 else 0 end),--有账单5966185
    sum(case when rpd_homepage_1w>=1 and travel is not null then 1 else 0 end),--活跃有账单812626
    sum(case when travel is not null and label>=1 and  rpd_homepage_1w>=1 then 1 else 0 end),--105675,0.1302
    sum(case when travel is not null and label<1 and  rpd_homepage_1w>=1 then 1 else 0 end)--706951
from 
    (select * from vdm_fin.lcl_loan_demand where rpd_homepage_1w>=1 or travel is not null) t;
--08-17
select sum(case when rpd_homepage_1w>=1 then 1 else 0 end),--一周活跃1508539
    sum(case when rpd_homepage_1w>=1 and rpd_loan_order_cnt>=1 then 1 else 0 end),--一周活跃贷款用户 519631
    sum(case when rpd_homepage_1w>=1 and rpd_loan_order_cnt>=1 and label>=1 then 1 else 0 end),--一周活跃贷款用户下单 98139
    sum(case when rpd_homepage_1w>=1 and label>=1 then 1 else 0 end),--一周活跃下周下单225942,0.1497 贷款:0.1888 未贷款:0.1292
    sum(case when travel is not null then 1 else 0 end),--有账单5439507
    sum(case when rpd_homepage_1w>=1 and travel is not null then 1 else 0 end),--活跃有账单1161674
    sum(case when travel is not null and label>=1 and  rpd_homepage_1w>=1 then 1 else 0 end),--225821,0.1943
    sum(case when travel is not null and label<1 and  rpd_homepage_1w>=1 then 1 else 0 end)--935853
from 
    (select * from vdm_fin.lcl_loan_demand where rpd_homepage_1w>=1 or travel is not null) t;


select user_id,gender，age_part,
    --负债属性
    user_debt_huabei_limit,user_debt_jd_limit,
    --账号属性
    is_info_verify,is_operator_verify,is_creqditbill_verify,
    rpd_fst_id_verify_time,reg_time,
    --借贷属性
    rpd_order_1w,rpd_order_3m,
    --行为属性
    rpd_homepage_1w,rpd_homepage_1m,rpd_homepage_3m,rpd_homepage_6m,
    --账单属性
    travel,outting,hotel,high_level_hotel,common_hotel,restaurant,high_level_restaurant,common_restaurant,cinema,entertainment,health_care,dressing,luxuries,clothing,brand_clothing,common_clothing,group_buying,hobby,education,pet,medical,logistics,wedding,mother_baby,supermarket,online_shopping,thirdparty_pay,bill_installment,car,high_level_car,home_living,q3c_consume,house,lottery,insurance,loan,investment,bank_business,trade,food,rent_house,home_decoration,housekeeping,literary,oversea,
    app_num,fin_app_num,fin_app_rate



