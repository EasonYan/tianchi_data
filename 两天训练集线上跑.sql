

create table train_item_no_geo as 
    select distinct item_id from tianchi_lbs.tianchi_mobile_recommend_train_item


create table y_train_12_16_12_17_raw as 
select * from tianchi_lbs.tianchi_mobile_recommend_train_user
where time >="2014-12-16" and time<"2014-12-18"

create table y_online_2day_train_time_info (user_id STRING,item_id STRING,
item_category STRING,sum_behavior BIGINT,sum_times1 BIGINT,sum_times2 BIGINT,
sum_times3 BIGINT,sum_times4 BIGINT,sum_hours1 BIGINT,sum_hours4 BIGINT,
time_decay_click_times DOUBLE,time_decay_collect_times DOUBLE,time_decay_shop_times DOUBLE,
time_decay_purchase_times DOUBLE,
click_average DOUBLE,click_ratio_buy DOUBLE,last_click_time_to_now BIGINT,
last_collect_time_to_now BIGINT,last_shop_time_to_now BIGINT,
last_purchase_time_to_now BIGINT,collect_shop_is_purchase BIGINT)

跑mapreduce


drop table if exists y_online_2dayt_train_category_info;
create table y_online_2dayt_train_category_info (user_id STRING,item_category STRING,
user_category_times1 BIGINT,user_category_times2 BIGINT,user_category_times3 BIGINT,
user_category_times4 BIGINT,latest_buy_item STRING)

跑user_mapreduce

醒来应该跑完

drop table if exists  y_online_2day_train_feature_add_category;
create table y_online_2day_train_feature_add_category as
select a.*, b.user_id as user_id2,b.item_category as item_category2,
b.user_category_times1 as user_category_times1
,b.user_category_times2 as user_category_times2,b.user_category_times3 as user_category_times3
,b.user_category_times4 as user_category_times4,b.latest_buy_item as latest_buy_item
from 
y_online_2day_train_time_info a left outer join y_online_2dayt_train_category_info b
on a.item_category=b.item_category and a.user_id = b.user_id


y
create table y_esaon_yanzheng_purchased_time_12_18_raw as
select distinct user_id,item_id from 
(
select * from y_all_purchased_set where 
time>="2014-12-18" and time<"2014-12-19"
)a

drop table if exists y_online_2day_train_feature_add_label;
create table y_online_2day_train_feature_add_label as
select a.*,b.user_id as user_id_yes_or_no,b.item_id as item_id_yes_or_no
from y_online_2day_train_feature_add_category a left outer join y_esaon_yanzheng_purchased_time_12_18_raw b on 
a.user_id = b.user_id and a.item_id = b.item_id


drop table if exists y_online_2day_feature_add_label_final;
create table y_online_2day_feature_add_label_final as
select udf(user_id,item_id,
item_category,sum_behavior,sum_times1,sum_times2,
sum_times3,sum_times4,sum_hours1,sum_hours4,
time_decay_click_times,time_decay_collect_times,time_decay_shop_times,
time_decay_purchase_times,
click_average,click_ratio_buy,last_click_time_to_now,
last_collect_time_to_now,last_shop_time_to_now,
last_purchase_time_to_now,collect_shop_is_purchase,
user_category_times1,user_category_times2,user_category_times3,
user_category_times4,latest_buy_item,
user_id_yes_or_no,item_id_yes_or_no)
as (user_id,item_id,
item_category,sum_behavior,sum_times1,sum_times2,
sum_times3,sum_times4,sum_hours1,sum_hours4,
time_decay_click_times,time_decay_collect_times,time_decay_shop_times,
time_decay_purchase_times,
click_average,click_ratio_buy,last_click_time_to_now,
last_collect_time_to_now,last_shop_time_to_now,
last_purchase_time_to_now,collect_shop_is_purchase,
category_popularity,latest_buy_item,shop_preferce,yes_buy)
from 
(
select * from y_online_2day_train_feature_add_label
)a;

drop table if exists y_online_2day_true_sample;
create table y_online_2day_true_sample as
select * from y_online_2day_feature_add_label_final where yes_buy =1

drop table if exists y_online_2day_negative_sample;
create table y_online_2day_negative_sample as
select * from y_online_2day_feature_add_label_final where yes_buy =0

y_online_2day_negative_sample_pickup


drop table if exists y_online_2day_true_sample_24;
create table y_online_2day_true_sample_24 as
select * from y_online_2day_true_sample where last_click_time_to_now<24


drop table if exists y_online_2day_mix_sample;
create table y_online_2day_mix_sample as
select * from
(
select * from y_online_2day_negative_sample_pickup
union all
select * from y_online_2day_true_sample
)a

drop table if exists y_online_2day_mix_sample_24;
create table y_online_2day_mix_sample_24 as
select * from
(
select * from y_eason2_negative_sample_pickup
union all
select * from y_online_2day_true_sample_24
)a


正样本:32万
负样本:3000000
create table y_train_12_17_12_18_raw as 
select * from tianchi_lbs.tianchi_mobile_recommend_train_user
where time >="2014-12-17" and time<"2014-12-19"

create table y_online_2day_test_time_info (user_id STRING,item_id STRING,
item_category STRING,sum_behavior BIGINT,sum_times1 BIGINT,sum_times2 BIGINT,
sum_times3 BIGINT,sum_times4 BIGINT,sum_hours1 BIGINT,sum_hours4 BIGINT,
time_decay_click_times DOUBLE,time_decay_collect_times DOUBLE,time_decay_shop_times DOUBLE,
time_decay_purchase_times DOUBLE,
click_average DOUBLE,click_ratio_buy DOUBLE,last_click_time_to_now BIGINT,
last_collect_time_to_now BIGINT,last_shop_time_to_now BIGINT,
last_purchase_time_to_now BIGINT,collect_shop_is_purchase BIGINT)


drop table if exists y_online_2day_test_category_info;
create table y_online_2day_test_category_info (user_id STRING,item_category STRING,
user_category_times1 BIGINT,user_category_times2 BIGINT,user_category_times3 BIGINT,
user_category_times4 BIGINT,latest_buy_item STRING)

drop table if exists  y_online_2day_test_feature_add_category;
create table y_online_2day_test_feature_add_category as
select a.*, b.user_id as user_id2,b.item_category as item_category2,
b.user_category_times1 as user_category_times1
,b.user_category_times2 as user_category_times2,b.user_category_times3 as user_category_times3
,b.user_category_times4 as user_category_times4,b.latest_buy_item as latest_buy_item
from 
y_online_2day_test_time_info a left outer join y_online_2day_test_category_info b
on a.item_category=b.item_category and a.user_id = b.user_id

drop table if exists y_online_2day_test_feature_add_label_final;
create table y_online_2day_test_feature_add_label_final as
select udf(user_id,item_id,
item_category,sum_behavior,sum_times1,sum_times2,
sum_times3,sum_times4,sum_hours1,sum_hours4,
time_decay_click_times,time_decay_collect_times,time_decay_shop_times,
time_decay_purchase_times,
click_average,click_ratio_buy,last_click_time_to_now,
last_collect_time_to_now,last_shop_time_to_now,
last_purchase_time_to_now,collect_shop_is_purchase,
user_category_times1,user_category_times2,user_category_times3,
user_category_times4,latest_buy_item,
"","")
as (user_id,item_id,
item_category,sum_behavior,sum_times1,sum_times2,
sum_times3,sum_times4,sum_hours1,sum_hours4,
time_decay_click_times,time_decay_collect_times,time_decay_shop_times,
time_decay_purchase_times,
click_average,click_ratio_buy,last_click_time_to_now,
last_collect_time_to_now,last_shop_time_to_now,
last_purchase_time_to_now,collect_shop_is_purchase,
category_popularity,latest_buy_item,shop_preferce,yes_buy)
from 
(
select * from y_online_2day_test_feature_add_category
)a;

drop table if exists y_online_2day_feature_sub_item_set;
create table y_online_2day_feature_sub_item_set as
select a.*,b.item_id as item_id_in_subset
from y_online_2day_test_feature_add_label_final a left outer join train_item_no_geo b on 
a.item_id = b.item_id 
where b.item_id is not null


drop table if exists y_online_2day_feature_sub_item_no_buy;
create table y_online_2day_feature_sub_item_no_buy as 
    select * from y_online_2day_feature_sub_item_set
    where sum_times4=0 and last_click_time_to_now<19;

结果
y_test_predict_final_result

create table tianchi_mobile_recommendation_predict as 
    select distinct user_id,item_id from
    (
        select * from y_test_predict_final_result  where prediction_result=1 order by
prediction_score desc limit 100000
    )b