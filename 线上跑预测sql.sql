'''
把训练集提取出来。
'''
单独去除双十二的数据。
第一种处理方式，采样一半的数据
双十二的所有数据一共有332696874条
当天购买次数有 7508897条
create table y_train_online_12_12_all_raw as
select * from tianchi_lbs.tianchi_mobile_recommend_train_user
where time >="2014-12-12" and time<"2014-12-13"

取12-10 到12-17号的数据
create table y_train_online_12_10_to_12_17_not_12_all_raw as
select * from tianchi_lbs.tianchi_mobile_recommend_train_user
where (time>="2014-12-10" and time<"2014-12-12") 
or(time>="2014-12-13" and time<="2014-12-17")

讲那个12-12采样的数据加到12-10到12-17号上面。

create table y_train_online_12_10_to_12_17_all_raw as
select * from
(
select * from y_train_online_12_10_to_12_17_not_12_all_raw
union all
select * from y_train_online_12_12_all_raw
)a

建立时间分布信息和基本对item的浏览信息表。

create table y_online_user_train_time_info (user_id STRING,item_id STRING,
item_category STRING,sum_behavior BIGINT,sum_times1 BIGINT,sum_times2 BIGINT,
sum_times3 BIGINT,sum_times4 BIGINT,sum_hourss1 BIGINT,sum_hours41 BIGINT,
click_average DOUBLE,click_ratio_buy DOUBLE,last_click_time_to_now BIGINT,
last_collect_time_to_now BIGINT,last_shop_time_to_now BIGINT,
last_purchase_time_to_now BIGINT,click_interval_hours DOUBLE,
collect_interval_hours DOUBLE,shop_interval_hours DOUBLE,
purchase_interval_hours DOUBLE,collect_shop_is_purchase BIGINT)
(没重复)

'''
先拿出item_category信息
'''
create table y_online_train_item_category_info as 
select item_category,sum(sum_times1) as u_times1,sum(sum_times2) as u_times2,
sum(sum_times3) as u_times3,sum(sum_times4) as u_times4 ,
(sum(sum_times1)*0.1+sum(sum_times2)+sum(sum_times3)+sum(sum_times4)*2) as popularity from 
y_online_user_train_time_info
group by item_category


'''
购买的信息
比如不同的item_cateogy,item_id 个数等
'''
create table y_online_train_user_buy_distribution as
select user_id, count(distinct item_id) as buy_item_id_num,count(distinct item_category)
as buy_category_nums from y_online_user_train_time_info where sum_times4>0 group by user_id

'''
用户信息
'''
create table y_online_user_train_behavior_info as 
select user_id,sum(sum_times1) as u_times1,sum(sum_times2) as u_times2,
sum(sum_times3) as u_times3,sum(sum_times4) as u_times4,sum(sum_times1+sum_times2+
sum_times3+sum_times4) as all_times,count(distinct item_id) as item_times,count(distinct item_category)
as category_times from 
y_online_user_train_time_info
group by user_id


create table y_online_user_train_behavior_info_all as
select a.*,b.user_id as user_id2,
b.buy_item_id_num as diff_buy_item_times,b.buy_category_nums
as diff_buy_category_nums
from 
y_online_user_train_behavior_info a left outer join y_online_train_user_buy_distribution b
on a.user_id=b.user_id;

create table y_online_user_train_item_id_info as 
select item_id,sum(sum_times1) as u_times1,sum(sum_times2) as u_times2,
sum(sum_times3) as u_times3,sum(sum_times4) as u_times4 from 
y_online_user_train_time_info
group by item_id

create table y_online_train_feature_add_category as
select a.*, b.item_category as item_category2,b.u_times1 as category_times1
,b.u_times2 as category_times2,b.u_times3 as category_times3
,b.u_times4 as category_times4,b.popularity as category_popularity
from 
y_online_user_train_time_info a left outer join y_online_train_item_category_info b
on a.item_category=b.item_category;

add item_info
create table y_online_train_feature_add_category_item as
select a.*,b.item_id as item_id2,b.u_times1 as item_times1
,b.u_times2 as item_times2,b.u_times3 as item_times3
,b.u_times4 as item_times4
from 
y_online_train_feature_add_category a left outer join y_online_user_train_item_id_info b
on a.item_id=b.item_id;

create table y_online_train_feature_add_category_item_user as
select a.*,b.user_id as user_id2,b.u_times1 as user_times1
,b.u_times2 as user_times2,b.u_times3 as user_times3
,b.u_times4 as user_times4,b.diff_buy_item_times as diff_buy_item_times,
b.diff_buy_category_nums as diff_buy_category_nums,b.item_times as action_item_times,
b.category_times as action_category_times
from 
y_online_train_feature_add_category_item a left outer join y_online_user_train_behavior_info_all b
on a.user_id=b.user_id

'''
找y值
'''
create table y_online_test_res_12_18_raw as 
select * from y_all_purchase_set where time>="2014-12-18"
and time<"2014-12-19"

create table y_online_res_result12_18_sub_item_set as 
select distinct user_id,item_id from
(
select a.*, b.item_id as item_id2
from y_online_test_res_12_18_raw a left outer join tianchi_lbs.tianchi_mobile_recommend_train_item b on 
a.item_id = b.item_id 
where b.item_id is not null
)a

create table y_online_train_feature_add_yes_no as
select a.*,b.user_id as user_id3,b.item_id as item_id3
from y_online_train_feature_add_category_item_user a left outer join y_online_test_res_12_18_raw b on 
a.user_id = b.user_id and a.item_id = b.item_id

drop table if exists y_online_final_feature;
create table y_online_final_feature as 
select udfa(user_id,item_id,item_category,sum_behavior,sum_times1,
sum_times2,sum_times3, sum_times4,sum_hourss1,sum_hours41,click_average,
click_ratio_buy,last_click_time_to_now,last_collect_time_to_now,
last_shop_time_to_now,last_purchase_time_to_now,click_interval_hours,
collect_interval_hours,shop_interval_hours,purchase_interval_hours,collect_shop_is_purchase,
category_popularity,item_times1,item_times2,item_times3,item_times4,user_times1,user_times2,
user_times3,user_times4,diff_buy_item_times,diff_buy_category_nums,action_item_times,
action_category_times,user_id3,item_id3) 
as (user_id,item_id,item_category,sum_behavior,sum_times1,
sum_times2,sum_times3, sum_times4,sum_hours1,sum_hours4,click_average,
click_ratio_buy,last_click_time_to_now,last_collect_time_to_now,
last_shop_time_to_now,last_purchase_time_to_now,click_interval_hours,
collect_interval_hours,shop_interval_hours,purchase_interval_hours,collect_shop_is_purchase,
category_popularity,
item_popularity,typ1_ratio,typ2_ratio,
typ3_ratio,typ4_ratio,user_all_times,all_purchase_times,purchase_lity,item_action_buy_rate,
category_action_buy_rate,action_item_times,action_category_times,yes)
from 
(
select * from y_online_train_feature_add_yes_no
)a;

10-17号训练集中
正样本只有293555条。
总共样本为602509410条。


数据预处理。清洗
购买次数 = 行为总次数
总次数在5000次以上然后 购买记录为0 的 1111085

create table y_online_final_feature_clean_1 as
SELECT * from y_online_final_feature where user_all_times>5000 and all_purchase_times>0
or user_all_times<=5000

create table y_online_final_feature_clean_2 as 
SELECT * from y_online_final_feature where (user_all_times<10 and all_purchase_times=0)
or user_all_times >=10


y_online_final_feature_clean_2_norm
正样本（293444）
create table y_online_train_true_sample as
select * from y_online_final_feature_clean_2_norm where yes =1


create table y_online_train_negative_sample as
select * from y_online_final_feature_clean_2_norm where yes =0

正负样本采样
30:160
负样本 表格:y_online_train_negative_sample_pickup

融合训练集合。
create table y_online_train_mix_sample as
select * from
(
select * from y_online_train_negative_sample_pickup
union all
select * from y_online_train_true_sample
)a

打乱样本。

y_online_train_mix_sample_random



create table y_online_test_data_12_16_17_18_raw as
select * from tianchi_lbs.tianchi_mobile_recommend_train_user
where time >="2014-12-16" and time<="2014-12-18"

create table y_online_user_test_time_info like y_online_user_train_time_info

create table y_online_test_item_category_info as 
select item_category,sum(sum_times1) as u_times1,sum(sum_times2) as u_times2,
sum(sum_times3) as u_times3,sum(sum_times4) as u_times4 ,
(sum(sum_times1)*0.1+sum(sum_times2)+sum(sum_times3)+sum(sum_times4)*2) as popularity from 
y_online_user_test_time_info
group by item_category

create table y_online_test_user_buy_distribution as
select user_id, count(distinct item_id) as buy_item_id_num,count(distinct item_category)
as buy_category_nums from y_online_user_test_time_info where sum_times4>0 group by user_id

create table y_online_user_test_behavior_info as 
select user_id,sum(sum_times1) as u_times1,sum(sum_times2) as u_times2,
sum(sum_times3) as u_times3,sum(sum_times4) as u_times4,sum(sum_times1+sum_times2+
sum_times3+sum_times4) as all_times,count(distinct item_id) as item_times,count(distinct item_category)
as category_times from 
y_online_user_test_time_info
group by user_id

create table y_online_user_test_behavior_info_all as
select a.*,b.user_id as user_id2,
b.buy_item_id_num as diff_buy_item_times,b.buy_category_nums
as diff_buy_category_nums
from 
y_online_user_test_behavior_info a left outer join y_online_test_user_buy_distribution b
on a.user_id=b.user_id;

create table y_online_user_test_item_id_info as 
select item_id,sum(sum_times1) as u_times1,sum(sum_times2) as u_times2,
sum(sum_times3) as u_times3,sum(sum_times4) as u_times4 from 
y_online_user_test_time_info
group by item_id

create table y_online_test_feature_add_category as
select a.*, b.item_category as item_category2,b.u_times1 as category_times1
,b.u_times2 as category_times2,b.u_times3 as category_times3
,b.u_times4 as category_times4,b.popularity as category_popularity
from 
y_online_user_test_time_info a left outer join y_online_test_item_category_info b
on a.item_category=b.item_category;

create table y_online_test_feature_add_category_item as
select a.*,b.item_id as item_id2,b.u_times1 as item_times1
,b.u_times2 as item_times2,b.u_times3 as item_times3
,b.u_times4 as item_times4
from 
y_online_test_feature_add_category a left outer join y_online_user_test_item_id_info b
on a.item_id=b.item_id;


create table y_online_train_feature_add_category_item_user as
select a.*,b.user_id as user_id2,b.u_times1 as user_times1
,b.u_times2 as user_times2,b.u_times3 as user_times3
,b.u_times4 as user_times4,b.diff_buy_item_times as diff_buy_item_times,
b.diff_buy_category_nums as diff_buy_category_nums,b.item_times as action_item_times,
b.category_times as action_category_times
from 
y_online_train_feature_add_category_item a left outer join y_online_user_train_behavior_info_all b
on a.user_id=b.user_id;




create table y_online_final_feature as 
select udfa(user_id,item_id,item_category,sum_behavior,sum_times1,
sum_times2,sum_times3, sum_times4,sum_hourss1,sum_hours41,click_average,
click_ratio_buy,last_click_time_to_now,last_collect_time_to_now,
last_shop_time_to_now,last_purchase_time_to_now,click_interval_hours,
collect_interval_hours,shop_interval_hours,purchase_interval_hours,collect_shop_is_purchase,
category_popularity,item_times1,item_times2,item_times3,item_times4,user_times1,user_times2,
user_times3,user_times4,diff_buy_item_times,diff_buy_category_nums,action_item_times,
action_category_times,user_id3,item_id3) 
as (user_id,item_id,item_category,sum_behavior,sum_times1,
sum_times2,sum_times3, sum_times4,sum_hours1,sum_hours4,click_average,
click_ratio_buy,last_click_time_to_now,last_collect_time_to_now,
last_shop_time_to_now,last_purchase_time_to_now,click_interval_hours,
collect_interval_hours,shop_interval_hours,purchase_interval_hours,collect_shop_is_purchase,
category_popularity,
item_popularity,typ1_ratio,typ2_ratio,
typ3_ratio,typ4_ratio,user_all_times,all_purchase_times,purchase_lity,item_action_buy_rate,
category_action_buy_rate,action_item_times,action_category_times,yes)
from 
(
select * from y_online_train_feature_add_yes_no
)a;

归一化
y_online_test__final_feature_norm

预测

y_online_test_predict_all_result

create table y_online_test_predict_sub_itemset as
select * from y_online_test_predict_all_result where prediction_result =1
order by prediction_score desc limit 1500000


create table y_online_test_predict_sub_itemset as
select * from y_online_test_predict_all_result where prediction_result =1
order by prediction_score desc limit 1100000;
create table tianchi_mobile_recommendation_predict as
select distinct user_id,item_id from 
(
select a.user_id as user_id,a.item_id as item_id,b.item_id as item_id2 
from y_online_test_predict_sub_itemset a left outer join tianchi_lbs.tianchi_mobile_recommend_train_item
b on 
a.item_id = b.item_id 
where b.item_id is not null
)a
select count(*)from tianchi_mobile_recommendation_predict