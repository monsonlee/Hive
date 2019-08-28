1、Hive避免使用MR的情况
=======================

在Hive中的查询，大多数情况下都会被解析成MapReduce的job进行计算。

Hive中有些情况是不会触发MapReduce的，即所谓的--Local mode本地模式。

以下三种方式都不会触发MapReduce

1、select \* from

(select 'limx' as name,'man' as gender,18 as age) t1;

![D:\\github\\Hive\\images\\1.1.png](media/e75634f97bf500cb406ef723db03d189.png)

或者

select \* from t1 where pt='20190101';（限制分区）

这种情况下，Hive会读取mytable1对应的存储目录下的文件。

2、select \* from mytable1 limit 10;

3、手动启用本地模式：

set hive.exec.mode.local.auto=true;

2、Hive控制map个数和reduce个数
==============================

2.1、控制hive任务中的map数
--------------------------

通常情况下，作业会通过input的目录产生一个或者多个map任务。

主要的决定因素有：
input的文件总个数，input的文件大小，集群设置的文件块大小(目前为128M,
可在hive中通过set dfs.block.size;命令查看到，该参数不能自定义修改)；

举例：

a)
假设input目录下有1个文件a,大小为780M,那么hadoop会将该文件a分隔成7个块（6个128m的块和1个12m的块），从而产生7个map数。

b)
假设input目录下有3个文件a,b,c,大小分别为10m，20m，130m，那么hadoop会分隔成4个块（10m,20m,128m,2m），从而产生4个map数，即如果文件大于块大小(128m)，那么会拆分，如果小于块大小，则把该文件当成一个块。

是不是map数越多越好？

答案是否定的。如果一个任务有很多小文件（远远小于块大小128m），则每个小文件也会被当做一个块，用一个map任务来完成，而一个map任务启动和初始化的时间远远大于逻辑处理的时间，就会造成很大的资源浪费。而且，同时可执行的map数是受限的。

是不是保证每个map处理接近128m的文件块，就高枕无忧了？

答案也是不一定。比如有一个127m的文件，正常会用一个map去完成，但这个文件只有一个或者两个小字段，却有几千万的记录，如果map处理的逻辑比较复杂，用一个map任务去做，肯定也比较耗时。

针对上面的问题3和4，我们需要采取两种方式来解决：即减少map数和增加map数。

如何合并小文件，减少map数？

假设一个SQL任务：

Select count(1)

from popt_tbaccountcopy_mes

where pt = ‘2012-07-04’;

该任务的inputdir
/group/p_sdo_data/p_sdo_data_etl/pt/popt_tbaccountcopy_mes/pt=2012-07-04共有194个文件，其中很多是远远小于128m的小文件，总大小9G，正常执行会用194个map任务。

Map总共消耗的计算资源： SLOTS_MILLIS_MAPS= 623,020。

我通过以下方法来在map执行前合并小文件，减少map数：

set mapred.max.split.size=100000000;

set mapred.min.split.size.per.node=100000000;

set mapred.min.split.size.per.rack=100000000;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

再执行上面的语句，用了74个map任务，map消耗的计算资源：SLOTS_MILLIS_MAPS=
333,500。

对于这个简单SQL任务，执行时间上可能差不多，但节省了一半的计算资源。

大概解释一下，100000000表示100M, set
hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;这个参数表示执行前进行小文件合并，前面三个参数确定合并文件块的大小，大于文件块大小128m的，按照128m来分隔，小于128m,大于100m的，按照100m来分隔，把那些小于100m的（包括小文件和分割大文件剩下的）进行合并，最终生成了74个块。

如何适当的增加map数？

当input的文件都很大，任务逻辑复杂，map执行非常慢的时候，可以考虑增加Map数，来使得每个map处理的数据量减少，从而提高任务的执行效率。

假设有这样一个任务：

Select data_desc,

count(1),

count(distinct id),

sum(case when …),

sum(case when …),

sum(…)

from a group by data_desc

如果表a只有一个文件，大小为120M，但包含几千万的记录，如果用1个map去完成这个任务，肯定是比较耗时的，这种情况下，我们要考虑将这一个文件合理的拆分成多个，这样就可以用多个map任务去完成。

set mapred.reduce.tasks=10;

create table a_1 as

select \* from a

distribute by rand(123);

这样会将a表的记录，随机的分散到包含10个文件的a_1表中，再用a_1代替上面sql中的a表，则会用10个map任务去完成。每个map任务处理大于12M（几百万记录）的数据，效率肯定会好很多。

看上去，貌似这两种有些矛盾，一个是要合并小文件，一个是要把大文件拆成小文件，这点正是重点需要关注的地方，根据实际情况，控制map数量需要遵循两个原则：使大数据量利用合适的map数；使单个map任务处理合适的数据量。

2.2、控制hive任务的reduce数
---------------------------

Hive自己如何确定reduce数：

reduce个数的设定极大影响任务执行效率，不指定reduce个数的情况下，Hive会猜测确定一个reduce个数，基于以下两个设定：

hive.exec.reducers.bytes.per.reducer（每个reduce任务处理的数据量，默认为1000\^3=1G）

hive.exec.reducers.max（每个任务最大的reduce数，默认为999）

计算reducer数的公式很简单N=min(参数2，总输入数据量/参数1)

即，如果reduce的输入（map的输出）总大小不超过1G,那么只会有一个reduce任务；

如：select pt,count(1) from popt_tbaccountcopy_mes where pt = ‘2012-07-04’ group
by pt;

/group/p_sdo_data/p_sdo_data_etl/pt/popt_tbaccountcopy_mes/pt=2012-07-04
总大小为9G多，因此这句有10个reduce

调整reduce个数方法一：

调整hive.exec.reducers.bytes.per.reducer参数的值；

set hive.exec.reducers.bytes.per.reducer=500000000; （500M）

select pt,count(1) from popt_tbaccountcopy_mes where pt = ‘2012-07-04’ group by
pt; 这次有20个reduce

调整reduce个数方法二；

set mapred.reduce.tasks = 15;

select pt,count(1) from popt_tbaccountcopy_mes where pt = ‘2012-07-04’ group by
pt;这次有15个reduce

reduce个数并不是越多越好；

同map一样，启动和初始化reduce也会消耗时间和资源；

另外，有多少个reduce,就会有多少个输出文件，如果生成了很多个小文件，那么如果这些小文件作为下一个任务的输入，则也会出现小文件过多的问题；

什么情况下只有一个reduce？

很多时候你会发现任务中不管数据量多大，不管你有没有设置调整reduce个数的参数，任务中一直都只有一个reduce任务；其实只有一个reduce任务的情况，除了数据量小于hive.exec.reducers.bytes.per.reducer参数值的情况外，还有以下原因：

a) 没有group by的汇总，比如把select pt,count(1) from popt_tbaccountcopy_mes
where pt = '2012-07-04' group by pt; 写成 select count(1) from
popt_tbaccountcopy_mes where pt = '2012-07-04';

这点非常常见，希望大家尽量改写。

b) 用了Order by

c) 有笛卡尔积

通常这些情况下，除了找办法来变通和避免，我暂时没有什么好的办法，因为这些操作都是全局的，所以hadoop不得不用一个reduce去完成；

同样的，在设置reduce个数的时候也需要考虑这两个原则：使大数据量利用合适的reduce数；使单个reduce任务处理合适的数据量。

3、Join
=======

3.1、inner join
---------------

3.2、left outer join
--------------------

3.3、right outer join
---------------------

3.4、full outer join
--------------------

3.5、left semi-join
-------------------

3.6、cross join（笛卡儿积）
---------------------------

3.7、map-side join
------------------

4、调优
=======

4.1、使用explain
----------------

4.2、explain extended
---------------------

4.3、限制调整
-------------

4.4、JOIN优化
-------------

4.5、本地模式
-------------

4.6、并行执行
-------------

4.7、严格模式
-------------

4.8、调整mapper个数和reducer个数
--------------------------------

4.9、JVM重用
------------

4.10、索引
----------

4.11、动态分区调整
------------------

4.12、推测执行
--------------

4.13、单个MR中多个GROUP BY
--------------------------

4.14、数据倾斜、任务长尾
------------------------

**join长尾**

背景

SQL在Join执行阶段会将Join
Key相同的数据分发到同一个执行Instance上处理。如果某个Key上的数据量比较多，会导致该Instance执行时间比其他Instance执行时间长。其表现为：执行日志中该Join
Task的大部分Instance都已执行完成，但少数几个Instance一直处于执行中，这种现象称之为长尾

**长尾类别&优化方法**

**小表长尾**

Join倾斜时，如果某路输入比较小，可以采用Mapjoin避免倾斜。Mapjoin的原理是将Join操作提前到Map端执行，这样可以避免因为分发Key不均匀导致数据倾斜。但是Mapjoin的使用有限制，必须是Join中的从表比较小才可用。所谓从表，即Left
Outer Join中的右表，或者Right Outer Join中的左表。

**热点值长尾**

如果是因为热点值导致长尾，并且Join的输入比较大无法用Mapjoin，可以先将热点Key取出，对于主表数据用热点Key切分成热点数据和非热点数据两部分分别处理，最后合并，union
all 热点和非热点的数据。

**空值长尾**

join时，假设左表(left_table)存在大量的空值，空值聚集到一个reduce上。由于left_table
存在大量的记录，无法使用mapjoin 。此时可以使用 coalesce(left_table.key,
rand()\*9999)将key为空的情况下赋予随机值，来避免空值集中造成长尾。

**map长尾**

Map端读取数据时，由于文件大小分布不均匀，一些map任务读取并处理的数据特别多，一些map任务处理的数据特别少，造成map端长尾。这种情形没有特别好的方法，只能调节splitsize来增加mapper数量，让数据分片更小，以期望获得更均匀的分配。

**reduce长尾**

Distinct执行原理：将需要去重的字段以及Group
By字段联合作为key将数据分发到Reduce端。

由于Distinct操作的存在，数据无法在Map端的Shuffle阶段根据Group
By先做一次聚合操作,减少传输的数据量，而是将所有的数据都传输到Reduce端，当Key的数据分发不均匀时，就会导致Reduce端长尾，特别当多个Distinct同时出现在一段SQL代码中时，数据会被分发多次，不仅会造成数据膨胀N倍，也会把长尾现象放大N倍。

**原sql：**

SELECT D1, D2

, COUNT(DISTINCT CASE

WHEN A IS NOT NULL THEN B

END) AS B_distinct_cnt

FROM xxx

GROUP BY D1, D2

**优化：**

create table tmp1

as

select D1,D2,B,

count( case when A is not null then B end ) as B_cnt

from xxx

group by D1, D1, B

select D1,D2,

sum(case when B_cnt \> 0 then 1 else 0 end) as B_distinct_cnt

from tmp1

group by D1,D2

5、窗口函数
===========

5.1、sum,avg,min,max
--------------------

Windows子句： Rows between

Preceding：往前

Following：往后

Current row：当前行

Unbounded：起点，unbounded preceding表示从前面的起点

unbounded following：表示到后面的终点

**函数功能：**实现分组内所有和连续累积的统计

**Sum：ROWS BETWEEN 7 PRECEDING and CURRENT ROW 往前7天+当前行**

SELECT catering_am_code,\`date\`

, num

, SUM(num) OVER (PARTITION BY catering_am_code ORDER BY \`date\` ROWS BETWEEN 7
PRECEDING and CURRENT ROW) num4

, SUM(num) OVER (PARTITION BY catering_am_code ORDER BY \`date\` ROWS BETWEEN 7
PRECEDING and CURRENT ROW)\*1.0/7 num5

FROM bt_yunli.ads_catering_am_performance_201906_mf

WHERE pt = '20190630'

![](media/ac7d6c95098f85edc73b1f8454205e67.png)

Avg，min，max同理

5.2、ntile,row_number,rank,dense_rank
-------------------------------------

Ntile(n)功能：将分组数据按照顺序切分成n片，返回当前切片值。Ntile不支持windows子句，比如rows
between

Row_number()功能：分组排序，从1开始

Rank()：分组排名，可能有空位，比如：两个第三名，那么下一位是第五名，没有第四名

Dense_rank()：分组排名，没有空位
