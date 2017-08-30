# coding:utf-8

import pandas as pd
from datetime import datetime, date, timedelta


def get_firstday_of_week(s):
    d = datetime.strptime(s, '%Y-%m-%d')
    f = d - timedelta(days=d.weekday())
    return f.strftime('%Y-%m-%d')

# data = pd.read_csv('query_result.csv')
# data = data[data.dt <= '2017-08-06']
# data['loan_amount'] = datanalysea['loan_amount'] / 10000.0
# data['dt'] = data['dt'].apply(lambda s: get_firstday_of_week(s))
# data.groupby('dt').sum().to_csv('user_new_week.csv')


def get_sql1():
    cols = ['is_travel', 'is_outting', 'is_hotel', 'is_high_level_hotel', 'is_common_hotel', 'is_restaurant', 'is_high_level_restaurant', 'is_common_restaurant', 'is_cinema', 'is_entertainment', 'is_health_care', 'is_dressing', 'is_luxuries', 'is_clothing', 'is_brand_clothing', 'is_common_clothing', 'is_group_buying', 'is_hobby', 'is_education', 'is_pet', 'is_medical', 'is_logistics',
            'is_wedding', 'is_mother_baby', 'is_supermarket', 'is_online_shopping', 'is_thirdparty_pay', 'is_bill_installment', 'is_car', 'is_high_level_car', 'is_home_living', 'is_3c_consume', 'is_house', 'is_lottery', 'is_insurance', 'is_loan', 'is_investment', 'is_bank_business', 'is_trade', 'is_food', 'is_rent_house', 'is_home_decoration', 'is_housekeeping', 'is_literary', 'is_oversea']
    print ','.join(['c.'+col[3:] for col in cols])
    exit()
    for col in cols:
        name = col.split('_', 1)[1]
        # print "sum(%s) as %s," %(col,name)

        # s = ("select '%s' as type,%s as value,count(1) as user_num,\n" +
        #      "\tsum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as user_loan_num,\n" +
        #      "\tsum(case when rpd_loan_order_cnt>=1 then 1 else 0 end)*1.0/count(1) as loan_rate,\n" +
        #      "\tsum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/sum(case when rpd_loan_order_cnt>=1 then 1 else 0 end) as reloan_rate,\n" +
        #      "\tsum(case when rpd_loan_order_cnt=1 then 1 else 0 end) as num1,\n" +
        #      "\tsum(case when rpd_loan_order_cnt=1 then 1 else 0 end)*1.0/count(1) as p1,\n" +
        #      "\tsum(case when rpd_loan_order_cnt>=2 then 1 else 0 end) as num2,\n" +
        #      "\tsum(case when rpd_loan_order_cnt>=2 then 1 else 0 end)*1.0/count(1) as p2\n" +
        #      "from vdm_fin.loan_user_analyse\n" +
        #      "where user_id is not null\n" +
        #      "group by %s\nunion all") % (col.split('_', 1)[1], col.split('_', 1)[1], col.split('_', 1)[1])
        # print s

get_sql1()
