'''
1:数据预处理
点击次数的分析。一月有30天。20天用于逛淘宝。每天上下午各两个小时。
每次点击页面加载时间和观看时间为4S，这样的话，一个用户每个月最多可以
产生的浏览记录条数为：6*4*300=9600条。一般大于这个数值就可以默认为爬虫。
可以直接去掉所有的记录。
'''
'''
首先构造下user-browse_times的表,sql如下。
'''
create table y_train_all_user as 
select user_id,count(user_id) as browse_times from y_train_time1123_1130_clean
group by user_id;


'''
前200名点击量的购买次数为4034条。
前100名为1806条
select sum(buy_times) from y_train1123_1130_purchase_user_set 
where user_id in
(
select user_id from
(select * from y_train_all_user order by browse_times desc limit 100)a
)

没有购买的点击量大于5000的条数为 947223条
select count(*) from y_train_time_11_23_to_11_30_raw 
where user_id in
(
select user_id from
(select * from y_train_1123_1130_user_nobuy_set where browse_times >5000)a
)

'''

'''预处理过滤用户可以用这样 就是什么去掉浏览次数在5000次以上然后什么都没买的。
浏览次数在5000以上然后买了少于3件的也去掉。

select a.user_id as user_id,a.browse_times as browse_times,b.user_id as bid,
b.buy_times as buy_times
from y_train_all_user a left outer join y_train1123_1130_purchase_user_set b on 
a.user_id = b.user_id 
where (b.user_id is null and a.browse_times >5000) or (a.browse_times>5000 and 
b.buy_times<3) 
'''
'''
然后取出爬虫用户
清洗掉之后还剩下
'''
create table y_train_time1123_1130_clean as 
select * from y_train_time_11_23_to_11_30_raw where user_id  not in
(
select user_id from 
(
select a.user_id as user_id,a.browse_times as browse_times,b.user_id as bid,
b.buy_times as buy_times
from y_train_all_user a left outer join y_train1123_1130_purchase_user_set b on 
a.user_id = b.user_id 
where (b.user_id is null and a.browse_times >5000) or (a.browse_times>5000 and 
b.buy_times<3) 
)b
)

'''
建立11-30号购买用户表，和购买的次数。
'''
create table y_train1123_1130_purchase_user_set as
select user_id,count(user_id) as buy_times from y_train_time1123_1130_clean
where behavior_type = 4 group by user_id;

'''
建立11-30号前没有买过产品的用户集合以及他们的浏览次数
'''
create table y_train_1123_1130_user_nobuy_set as
select a.user_id as user_id,a.browse_times as browse_times,b.user_id as bid
from y_train_all_user a left outer join y_train1123_1130_purchase_user_set b on 
a.user_id = b.user_id 
where b.user_id is null

'''
建立由user_id,item_id,item_categoty,behavior_type为分组的用户信息和对应的次数。
用来后面计算信息表的。(条)
'''
create table y_trian_1123_1130_2_3_4_groupby_type as
select user_id,item_id,item_category,behavior_type,count(*) as nums from
y_train_time1123_1130_clean
group by user_id,item_id,behavior_type,item_category

'''
接下来计算
整个info 表
使用mapreduce 里的Count_infoMapper 和Count_infoReducer
count_info 过程花了(23分钟)
先建立目标表。
其实times和时间信息可以一起算
'''

create table y_train_user_type_2_3_4count_times like y_user_type_2_3_4count_times

'''
接下来计算时间信息，同样用mapreduce来计算。
这次加了个combiner.减少map输出相同的值，降低了网络传输速率。
分别为Time_infoMapper 和Time_infoReducer
主要提取的时间上的特征为
用户对item的浏览时间，
用户的平均每小时浏览次数。
用户购买次数
'''
'''
先建立目标的时间表
'''

-------------------------------------------------------------------

'''
融合特征阶段。
y_train_user_type_2_3_4count_times 表中有
用户对每个item的浏览次数，收藏次数，点击次数
sum_behavior,sum_times1,sum_times2,sum_times3,sum_times4,sum_hours1,sum_hours4,average_click,click_buy_rate
'''

'''
分析:由于存在很多用户，点击次数都少于那个购买次数，
所以用
购买item 的次数/总购买的次数。
和
购买的次数 /在这个类别里面的次数。
接下来计算那个啥，item_category被点击，收藏，购买，的次数。
首先create 通过groupby type 算好的表。
'''
create table y_train_user_data_group_by_category as
select item_category,behavior_type,count(*) as nums from
y_train_time1123_1130_clean
group by item_category


create table y_train_item_category_info as 
select item_category,sum(sum_times1) as u_times1,sum(sum_times2) as u_times2,
sum(sum_times3) as u_times3,sum(sum_times4) as u_times4 ,(sum(sum_times1)*0.1+sum(sum_times2)+sum(sum_times3)+sum(sum_times4)*2) as popularity from 
y_user_train_time_info
group by item_category


'''
查看用户购买的产品属于哪几个类别
'''
create table y_train_user_buy_distribution as
select user_id, count(distinct item_id) as buy_item_id_num,count(distinct item_category)
as buy_category_nums from y_user_train_time_info where sum_times4>0 group by user_id
'''
用户总行为信息
'''
create table y_user_train_behavior_info as 
select user_id,sum(sum_times1) as u_times1,sum(sum_times2) as u_times2,
sum(sum_times3) as u_times3,sum(sum_times4) as u_times4,sum(sum_times1+sum_times2+
sum_times3+sum_times4) as all_times,count(distinct item_id) as item_times,count(distinct item_category)
as category_times from 
y_user_train_time_info
group by user_id

'''
用户购买产品所属的类别信息加到用户总行为信息表里面
'''
create table y_user_train_behavior_info_all as
select a.*,b.user_id as user_id2,
b.buy_item_id_num as diff_buy_item_times,b.buy_category_nums
as diff_buy_category_nums
from 
y_user_train_behavior_info a left outer join y_train_user_buy_distribution b
on a.user_id=b.user_id;

create table y_user_train_item_id_info as 
select item_id,sum(sum_times1) as u_times1,sum(sum_times2) as u_times2,
sum(sum_times3) as u_times3,sum(sum_times4) as u_times4 from 
y_user_train_time_info
group by item_id


'''
整合关联特征啦。category的信息
加入
'''
create table y_train_feature_add_category as
select a.*, b.user_id as user_id2,b.item_category as item_category2,b.u_times1 as category_times1
,b.u_times2 as category_times2,b.u_times3 as category_times3
,b.u_times4 as category_times4,b.popularity as category_popularity
from 
y_user_train_time_info a left outer join y_train_item_category_info b
on a.item_category=b.item_category

'''
整合关联特征，加入 item_id的统计信息
'''
create table y_train_feature_add_category_item as
select a.*,b.item_id as item_id2,b.u_times1 as item_times1
,b.u_times2 as item_times2,b.u_times3 as item_times3
,b.u_times4 as item_times4
from 
y_train_feature_add_category a left outer join y_user_train_item_id_info b
on a.item_id=b.item_id;

'''
整合关联特征，加入user_id的统计信息
'''
create table y_train_feature_add_category_item_user as
select a.*,b.user_id as user_id2,b.u_times1 as user_times1
,b.u_times2 as user_times2,b.u_times3 as user_times3
,b.u_times4 as user_times4,b.diff_buy_item_times as diff_buy_item_times,
b.diff_buy_category_nums as diff_buy_category_nums,b.item_times as action_item_times,
b.category_times as action_category_times
from 
y_train_feature_add_category_item a left outer join y_user_train_behavior_info_all b
on a.user_id=b.user_id;


create table y_feature_final1 as 
select udfa(user_id,item_id,item_category,sum_behavior,sum_times1,
sum_times2,sum_times3, sum_times4,sum_hours1,sum_hours4,click_average,
click_ratio_buy,item_category2,category_times1,category_times2,category_times3,
category_times4,category_popularity,item_id2,
item_times1,item_times2,item_times3,item_times4,user_id2,
user_times1,user_times2,user_times3,user_times4,diff_buy_item_times,
diff_buy_category_nums,action_item_times,action_category_times) as (user_id,item_id,item_category,sum_behavior,sum_times1,
sum_times2,sum_times3, sum_times4,sum_hours1,sum_hours4,click_average,
click_ratio_buy,category_popularity,item_popularity,typ1_ratio,typ2_ratio,
typ3_ratio,typ4_ratio,user_all_times,all_purchase_times,purchase_lity,item_action_buy_rate,category_action_buy_rate,action_item_times,action_category_times,yes)
from 
(
select * from y_train_feature_add_category_item_user
)a;

'''
初次特征集分析：495365448条
'''

'''
上面的clean貌似没有去掉很多东西，所以这里要再次设定过滤规则。
1：select count(*) from y_feature_final1 where user_all_times>5000 and all_purchase_times =0;（726058条）
2：select count(*) from y_feature_final1 where user_all_times<30 and all_purchase_times <2;（4505687）=0有(3798277条)
3：select count(*) from y_feature_final1 where user_all_times>5000 and all_purchase_times <3;（787041）
'''

'''
过滤规则:
1:create table y_train_feature_clean1 as 
select * from y_feature_final1 where (user_all_times >5000 and all_purchase_times>4)
or user_all_times<5000
2:create table y_train_feature_clean2 as
select * from y_train_feature_clean1 where (user_all_times <30 and all_purchase_times>2)
or user_all_times>30
'''
过滤掉后一周 user_id--item_id的条数为  495365448

feature 弄完后要弄对应的y值
首先建立12-01的股买记录

create table y_test_purchased_time_12_01_raw as
select distinct uesr_id,item_id from 
(
select * from y_all_purchased_set where 
time>="2014-12-01" and time<="2014-12-02"
)a
把第八天有买的user——ID，item_id加进去

create table y_train_feature_add_yes_no as
select a.*,b.user_id as user_id3,b.item_id as item_id3
from y_train_feature_clean2 a left outer join y_test_purchased_time_12_01_raw b on 
a.user_id = b.user_id and a.item_id = b.item_id
跑udf,把每条对应的有就设置为1，没有就设置为0

create table y_feature_final_sample as 
select udfa(user_id,item_id,item_category,sum_behavior,sum_times1,
sum_times2,sum_times3, sum_times4,sum_hours1,sum_hours4,click_average,
click_ratio_buy,category_popularity,item_popularity,typ1_ratio,typ2_ratio,
typ3_ratio,typ4_ratio,user_all_times,all_purchase_times,purchase_lity,item_action_buy_rate,
category_action_buy_rate,action_item_times,action_category_times,yes,user_id2,item_id2) 
as (user_id,item_id,item_category,sum_behavior,sum_times1,
sum_times2,sum_times3, sum_times4,sum_hours1,sum_hours4,click_average,
click_ratio_buy,category_popularity,item_popularity,typ1_ratio,typ2_ratio,
typ3_ratio,typ4_ratio,user_all_times,all_purchase_times,purchase_lity,item_action_buy_rate,
category_action_buy_rate,action_item_times,action_category_times,yes_buy)
from 
(
select * from y_train_feature_add_yes_no
)a;

归一化：
y_feature_final_sample_norm

                                
'''
负样本集  479516165条
'''
create table y_train_negative_sample as
select * from y_feature_final_sample_norm where yes_buy =0
'''
正样本
'''
create table y_train_true_sample as
select * from y_feature_final_sample_norm where yes_buy =1

create table y_train_negative_sample_not_norm as
select * from y_feature_final_sample where yes_buy =0
create table y_train_true_sample_not_norm as
select * from y_feature_final_sample where yes_buy =1

y_train_negative_sample_pickup_not_norm

'''
随机采样
true:200 0000
negative:12000000
'''
y_train_true_sample_pickup

y_train_negative_sample_pickup

'''
正负样本融合
'''
create table y_train_mix_sample as
select * from
(
select * from y_train_negative_sample_pickup
union all
select * from y_train_true_sample
)a

create table y_train_mix_sample_nt_norm as
select * from
(
select * from y_train_negative_sample_pickup_not_norm
union all
select * from y_train_true_sample_not_norm
)a
跑模型

'''
以下是测试样本的构建，额训练样本差不多。
'''

'''
建模完后就开始构建测试样本到特征空间的转换
'''
create table y_test_data_12_01_02_03_raw as
select * from tianchi_lbs.tianchi_mobile_recommend_train_user
where time >="2014-12-01" and time<"2014-12-04"

-- '''
-- 去爬虫用户。条大概
-- '''
-- create table y_test_data_clean_scrap as 
-- select * from y_test_data_12_01_12_01_raw where user_id not in
-- (
-- select user_id from y_user_train_behavior_info_all where u_times4=0 and all_times>5000
-- ) 
'''
先建立目标表。
'''
create table y_user_test_time_info like y_user_train_time_info

'''
跑mapreduce后是
条
'''

'''
查看用户购买的产品属于哪几个类别
'''
create table y_test_user_buy_distribution as
select user_id, count(distinct item_id) as buy_item_id_num,count(distinct item_category)
as buy_category_nums from y_user_test_time_info where sum_times4>0 group by user_id

'''
用户总行为信息
'''
create table y_user_test_behavior_info as 
select user_id,sum(sum_times1) as u_times1,sum(sum_times2) as u_times2,
sum(sum_times3) as u_times3,sum(sum_times4) as u_times4,sum(sum_times1+sum_times2+
sum_times3+sum_times4) as all_times,count(distinct item_id) as item_times,count(distinct item_category)
as category_times from 
y_user_test_time_info
group by user_id

create table y_user_test_behavior_info_all as
select a.*,b.user_id as user_id2,
b.buy_item_id_num as diff_buy_item_times,b.buy_category_nums
as diff_buy_category_nums
from 
y_user_test_behavior_info a left outer join y_test_user_buy_distribution b
on a.user_id=b.user_id;


'''
test item_id info
'''
create table y_user_test_item_id_info as 
select item_id,sum(sum_times1) as u_times1,sum(sum_times2) as u_times2,
sum(sum_times3) as u_times3,sum(sum_times4) as u_times4 from 
y_user_test_time_info
group by item_id


'''
test集合 category的信息
'''
create table y_test_item_category_info as 
select item_category,sum(sum_times1) as u_times1,sum(sum_times2) as u_times2,
sum(sum_times3) as u_times3,sum(sum_times4) as u_times4 ,(sum(sum_times1)*0.1+sum(sum_times2)+sum(sum_times3)+sum(sum_times4)*2) as popularity from 
y_user_test_time_info
group by item_category

'''
test add category_info
'''
create table y_test_feature_add_category as
select a.*, b.item_category as item_category2,b.u_times1 as category_times1
,b.u_times2 as category_times2,b.u_times3 as category_times3
,b.u_times4 as category_times4,b.popularity as category_popularity
from 
y_user_test_time_info a left outer join y_test_item_category_info b
on a.item_category=b.item_category;



create table y_test_feature_add_category_item as
select a.*,b.item_id as item_id2,b.u_times1 as item_times1
,b.u_times2 as item_times2,b.u_times3 as item_times3
,b.u_times4 as item_times4
from 
y_test_feature_add_category a left outer join y_user_test_item_id_info b
on a.item_id=b.item_id;

create table y_test_feature_add_category_item_user as
select a.*,b.user_id as user_id2,b.u_times1 as user_times1
,b.u_times2 as user_times2,b.u_times3 as user_times3
,b.u_times4 as user_times4,b.diff_buy_item_times as diff_buy_item_times,
b.diff_buy_category_nums as diff_buy_category_nums,b.item_times as action_item_times,
b.category_times as action_category_times
from 
y_test_feature_add_category_item a left outer join y_user_test_behavior_info_all b
on a.user_id=b.user_id;

create table y_test_feature_final1 as 
select udfa(user_id,item_id,item_category,sum_behavior,sum_times1,
sum_times2,sum_times3, sum_times4,sum_hours1,sum_hours4,click_average,
click_ratio_buy,item_category2,category_times1,category_times2,category_times3,
category_times4,category_popularity,item_id2,
item_times1,item_times2,item_times3,item_times4,user_id2,
user_times1,user_times2,user_times3,user_times4,diff_buy_item_times,
diff_buy_category_nums,action_item_times,action_category_times) as (user_id,item_id,item_category,sum_behavior,sum_times1,
sum_times2,sum_times3, sum_times4,sum_hours1,sum_hours4,click_average,
click_ratio_buy,category_popularity,item_popularity,typ1_ratio,typ2_ratio,
typ3_ratio,typ4_ratio,user_all_times,all_purchase_times,purchase_lity,item_action_buy_rate
,category_action_buy_rate,action_item_times,action_category_times,yes)
from 
(
select * from y_test_feature_add_category_item_user
)a;

去掉不活跃的家伙
create table y_test_feature_clean1 as
select * from y_test_feature_final1 where (user_all_times <20 and all_purchase_times<=1)
or user_all_times>20

归一化
y_test_feature_norm

预测结果表:
y_test_predict_all_result

'''
验证结果 算 F1
'''
'''
把12-03那天的购买记录提出来
一共()
'''
create table y_res_true_buy_12_04 as
select * from y_all_purchased_set
where time >="2014-12-04" and time<"2014-12-05"

'''
找出商品子集的购买内容
即 user_id,item_id 
并去重的。
'''

create table y_res_result_sub_item_set as 
select distinct user_id,item_id from
(
select a.*, b.item_id as item_id2
from y_res_true_buy_12_04 a left outer join tianchi_lbs.tianchi_mobile_recommend_train_item b on 
a.item_id = b.item_id 
where b.item_id is not null
)a


select count(*) from y_test_predict_all_result where prediction_result=1

预测结果属于1的集合
create table y_test_predict_sub_itemset as
select * from y_test_predict_all_result where prediction_result =1
order by prediction_score desc limit 2000000

建立预测集合属于商品子集同时去掉重复的集合。
create table tianchi_mobile_recommendation_predict as
select distinct user_id,item_id from 
(
select a.user_id as user_id,a.item_id as item_id,b.item_id as item_id2 
from y_test_predict_sub_itemset a left outer join tianchi_lbs.tianchi_mobile_recommend_train_item
b on 
a.item_id = b.item_id 
where b.item_id is not null
)a
(161430条)

算F1，recall, prediction

-- select * from y_test_predict_sub_itemset limit 100;
-- create table y_test_predict_final_result as
-- select distinct user_id,item_id from 
-- (
-- select a.user_id as user_id,a.item_id as item_id,b.item_id as item_id2 
-- from y_test_predict_sub_itemset a left outer join tianchi_lbs.tianchi_mobile_recommend_train_item
-- b on 
-- a.item_id = b.item_id 
-- where b.item_id is not null
-- )a

select count(*) from
(
select b.*
from y_test_predict_final_result b
inner join y_res_result_sub_item_set a
on a.user_id=b.user_id
and a.item_id=b.item_id
)a


select count(*) from 
(
select a.user_id as user_id,a.item_id as item_id,b.user_id as user_id2,b.item_id as item_id2
from y_test_predict_final_result a left outer join y_res_result_sub_item_set b on 
a.user_id = b.user_id and a.item_id=b.item_id
where b.user_id is not null
)a