DQuery 简介

DQuery 是 DPark 的 SQL 接口，主要为简单的交互数据分析设计，目前提供命令行界面。

如何使用

```
 $ dquery
 Welcome to DQuery 0.1, enjoy SQL and DPark! type 'help' for help.
 >> 
``` 
输入 `help` 即可显示在线帮助，目前只有基本的语法。

目前支持的 SQL 语句：
```
CREATE TABLE [IF NOT EXISTS] tbl_name [(create_definition, ...)] rdd_expr | select_statement

DROP TABLE [IF EXISTS] tbl_name, ...

SHOW TABLES [LIKE 'pattern']

SELECT expr FROM name_or_expr [[INNER | LEFT OUTER] JOIN tbl_name ON ] [WHERE condition] [GROUP BY cols] [HAVING condition] [ORDER BY cols [ASC | DESC]] [LIMIT n]
```

DQuery 是将SQL 语句解析后翻译成Python代码并调用DPark来做并行计算，SQL 中的表达式就是 Python 表达式。

创建表时默认的数据类型是RDD中的类型，如果指定的字段类型的话，就会尝试做类型转化。比如rdd_expr 是
textFile() 或者 csvFile(), 而字段指定为 field1 int, 则会尝试将字符串转为int, 查询时该字段就是intt类型。

SELECT 语句的语法可以参考Big Query 的文档：https://developers.google.com/bigquery/docs/query-reference
其中 DQuery 支持的汇聚函数有：min, max, avg, sum, last, count, adcount, group_concat, top.
普通的一元函数是Python的各种内置函数，以及 math 库的各种数值函数。

DQuery 启动时，会尝试加载全局定义的表(/etc/dquery) 以及当前用户 HOME 目录下的 .query。
在 DQuery 中创建的表也会自动写入到 .query 中，方便以后接着使用。

#举例：

在 /mfs/log/scribe/dpark/ 中有收集的dpark 任务的日志，是文本格式的，各个字段用空格做分隔符，先创建一个表：

```
create table dpark_log (date, time, user, cmd, job, job_id int, n_task int, avg_task_time float, lbytes long, rbytes long) textFile('/mfs/log/scribe/dpark/', followLink=False);
```

然后分析每天的计算用时和IO总量：
```
>> select sum(n_task * avg_task_time), sum(lbytes+rbytes) from dpark_log group by date order by date;
2013-05-06 16:49:07,496 [INFO] [scheduler] Got job 1 with 13 tasks: <MappedRDD <MappedRDD <MappedRDD <UnionRDD 13 <TextFileRDD /home2/scribe/dpark/theoden/dpark-2013-04-24_00000> ...>>>>
2013-05-06 16:49:10,060 [INFO] [job      ] Job 1 finished in 2.6s: min=0.1s, avg=0.3s, max=0.5s, maxtry=1
2013-05-06 16:49:10,060 [INFO] [job      ] read 16.4MB (59% localized)
2013-05-06 16:49:10,138 [INFO] [scheduler] Got job 2 with 12 tasks: <MappedRDD <ShuffledRDD <MappedRDD <MappedRDD <MappedRDD <UnionRDD 13 <TextFileRDD /home2/scribe/dpark/theoden/dpark-2013-04-24_00000> ...>>>>>>
2013-05-06 16:49:10,331 [INFO] [job      ] Job 2 finished in 0.2s: min=0.0s, avg=0.1s, max=0.1s, maxtry=1
2013-05-06 16:49:10,378 [INFO] [scheduler] Got job 3 with 12 tasks: <ParallelCollection 13>
2013-05-06 16:49:10,502 [INFO] [job      ] Job 3 finished in 0.1s: min=0.0s, avg=0.0s, max=0.1s, maxtry=1
-----------------------------------------------
| 2013-04-24 | 5429147.25101 | 16222410776614 |
| 2013-04-25 |  9426513.3809 | 33812007193882 |
| 2013-04-26 | 7248128.56607 | 22505428130906 |
| 2013-04-27 | 7729533.45016 | 22737401505201 |
| 2013-04-28 | 11556909.2569 | 28812919811210 |
| 2013-04-29 | 4476712.17074 | 16807503442524 |
| 2013-04-30 | 4098036.24926 | 16307838036411 |
| 2013-05-01 | 6656151.37464 | 20705927969166 |
| 2013-05-02 | 12018240.9765 | 32546460411736 |
| 2013-05-03 |  9723671.3852 | 31970891115262 |
| 2013-05-04 | 11150120.6009 | 38919923874343 |
| 2013-05-05 | 7665800.98158 | 21241371970205 |
| 2013-05-06 | 8926909.74505 | 35555625587343 |
-----------------------------------------------
```

如果想让结果更可读，可以将结果的量纲进行调整，得到实际使用的CPU数和以T为单位的IO，如下：

```
>> select sum(n_task * avg_task_time/80000.0), sum((lbytes+rbytes)/1e12) from dpark_log group by date order by date;
----------------------------------------------
| 2013-04-24 | 67.8643406376 | 16.2224107766 |
| 2013-04-25 | 117.831417261 | 33.8120071939 |
| 2013-04-26 | 90.6016070758 | 22.5054281309 |
| 2013-04-27 |  96.619168127 | 22.7374015052 |
| 2013-04-28 | 144.461365711 | 28.8129198112 |
| 2013-04-29 | 55.9589021343 | 16.8075034425 |
| 2013-04-30 | 51.2254531158 | 16.3078380364 |
| 2013-05-01 |  83.201892183 | 20.7059279692 |
| 2013-05-02 | 150.228012207 | 32.5464604117 |
| 2013-05-03 | 121.545892315 | 31.9708911153 |
| 2013-05-04 | 139.376507511 | 38.9199238743 |
| 2013-05-05 | 95.8225122698 | 21.2413719702 |
| 2013-05-06 | 111.586371813 | 35.5556255873 |
----------------------------------------------
```

如果想看出现次数最多的脚本(近似），可以这样
```
>> select top(cmd) from dpark_log;
------------------------------------------------------------------------------------
|                /mfs/user/wangqingyun/new_clickrate/jasper_rate_report.py | 75389 |
|                              /mfs/user/dengjian/guess/src/guess_admin.py | 52555 |
|               /mfs/user/erebor/online/Istari/src/report/erebor_weblog.py |  4844 |
|               /mfs/user/erebor/online/girion/src/plan/ctr_online_data.py |  3904 |
| /home/liuzhengyang/Istari/src/report/redis_weblog_cluster_hourlycheck.py |  3493 |
|                                                             grid_para.py |  2450 |
|                       /mfs/alg/rivendell/radio/algorithm/radio_report.py |  2361 |
|                     /mfs/alg/projects/AntiSpammer/online/movie_rating.py |  2345 |
|      /mfs/datasupport/xiliang_moria/agg_fact_web_log_session_nurl_day.py |  1708 |
|                                                       group_persona_2.py |  1636 |
|                /mfs/user/lianyijiang/tmp/DocCluster//vec_model/dfortf.py |  1321 |
|                         /mfs/user/liaofeng/script/calculate_hot_topic.py |  1297 |
|            /mfs/user/lianyijiang/tmp/DocCluster//prepare_data/dpurify.py |  1082 |
|                                                                  test.py |  1004 |
|                                                           sample_item.py |   988 |
|               /mfs/user/limeng/rivendell/radio/algorithm/radio_report.py |   820 |
|                                               new_subjects_clustering.py |   746 |
|                  /mfs/user/yanjun/neighbor/bin/data_neighbor_intimacy.py |   637 |
|                                                      tools/sync_table.py |   636 |
|                                                            sync_table.py |   610 |
------------------------------------------------------------------------------------
```

如果想看使用 CPU 时间最多的用户，
```
>> select sum(n_task * avg_task_time/80000.0) as cpus from dpark_log group by user order by cpus desc limit 10;
--------------------------------
|        mesos | 343.757984748 |
|     dengjian | 246.569538135 |
|  datasupport | 174.733468952 |
|          alg | 153.570239293 |
|       hezhao | 107.533595233 |
| shangxuejing | 49.7866056407 |
|      shenfei | 45.5115973775 |
|      xiliang | 32.0368595824 |
|        shire | 28.1172712128 |
|      wanghao | 23.2745788098 |
--------------------------------
```

#注意事项：

目前 SQL 的语法 解析是用正则表达式做的，当有语法错误时，可能会导致各种奇怪的错误。
如果运行结果不是你预期的，请仔细检查SQL语句是否合理，可以发到 #dpark 上讨论。

现在只在 theoden 上安装和部署了 DQuery，可以直接输入 dq 或者 dquery 来运行，DPark 的各种命令行参数都是有效的，默认会使用 flet6 集群来运行。
