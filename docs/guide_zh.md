# Dpark 编程指南

Dpark 是 Spark 的 Python 克隆，一个轻量的分布式计算框架，类似于MapReduce，
比它更灵活，可以用Python非常方便地进行分布式计算，并且提供了更多的功能以便
更好地进行迭代式计算。

## 开始使用
在用于离线计算的机器上已经部署了dpark, 可以直接使用：
(theoden,balin,dwalin,alg221-8)

from dpark import DparkContext
dpark = DparkContext()

## 运行

不加任何参数时，是以单机单线程方式运行，方便调试：

$ python examples/demo.py

调试完毕后，加上 -m 参数，就可以提交到 Mesos 上进行分布式计算：

$ python examples/demo.py -m mesos

在 Mesos 上运行时，需要在 alg221-8 机器上有提交程序的用户的账号，如果没有，
可以用公用账号 mesos 来运行：

$ sudo -u mesos python examples/demo.py -m mesos

也可以在单机用多进程方式计算：

$ python examples/demo.py -m process -p 4

## 命令行参数

当应用脚本有自己的命令行参数时，需要将Dpark 的参数写在前面，并用 '--' 区分开，比如:

$ python myscript.py -m mesos -- -f output

同时，在程序里需要先初始化DparkContext对象，然后再解析自己的参数。

## 弹性分布式数据集 RDD

Dpark 的核心概念是弹性分布式数据集(RDD), 它是支持容错的并行变换和操作的数据集合。
一个RDD由多个Split组成，它是进行并行计算的基本单位。

目前实现了两种RDD数据源：并行化的数据集，将一个普通的list拆成若干块得到；另一个
是存储在MFS上的单个或者多个文件，目前支持文本格式或者csv格式。

以上两种RDD通过并行的变换得到新的RDD，它们都支持同样的变换和操作。

### 集合

并行数据集通过 DparkContext().parallelize() 得到，可以指定拆分的块数，比如：

>>> l = range(10000)
>>> dl = dpark.paralleize(l, 10)

它表示对 l 拆成 10 个 split, 组成一个 RDD，然后就可以对 dl 进行并行计算，
比如 dl.reduce(lambda x,y:x+y) 就可以对它进行求和。

### 文件

Dpark 可以由存储在分布式文件系统（比如MFS)上的文件来创建RDD，支持文本或者CSV格式，
未来会加入更多的格式支持。

创建文件RDD的方法为：

>>> weblog = dpark.textFile("/mfs/log/weblog/2011/10/01")
>>> oct_log = dpark.csvFile("/mfs/log/weblog/2011/10/", splitSize=256<<20)

然后就可以对它进行并行变换和操作，比如计算一天的PV:

>>> print weblog.map(lambda x:1).reduce(lambda x,y:x+y)

创建文件RDD时可以指定分块大小 splitSize, 默认为 64M, 文件扩展名等，详见 dpark/context.py

如果需要访问其它数据源，比如MySQL，可以仿照 dpark/rdd.py 中TextFileRDD 来实现对应
的 RDD，不难实现。


### RDD 支持的变换和操作

RDD 支持两种并行运算：变换（从一个RDD经过某种运算得到另外一个RDD, 比如map)和操作（对一个
RDD进行聚合运算并立即返回结果到主程序，比如reduce)。

所有的变换都是惰性的，当调用变换函数的时候，并没有立即执行对应的计算，而是生成了一个携带
着足够计算信息的新的RDD（包括源RDD和变换函数），当对RDD进行操作时需要某个变换的结果时，
它才开始计算。这样，Dpark 可以自动合并若干个变换以同时运行，最大限度地降低不必要的数据传输。
另外，这样还可以容错，当某个部分计算失败，它拥有足够的信息以从新开始计算。

一个非常重要的变换是cache(). 当对某一个RDD进行cache()变换后，它的计算结果会缓存在各个计算
节点的共享内存中，下一次再需要该RDD的计算结果时，就可以直接从共享内存中获得，或者从新计算。
当某一个RDD的结果会被反复用到时，这可以大大加快计算，对迭代计算非常有利，在命令行交互执行
时也非常有用。

目前支持的 RDD 变换如下：

map(func)     : 对每个元素执行 func, 返回新的RDD
filter(func)  : 用 func 来过滤每个元素
flatMap(func) : 对每个元素执行 func, 将它返回的任意个元素组成新的RDD
union(rdds)   : 将多个rdd串起来组成新的
__slice__(i,j): 返回RDD的一部分区间
glom()        : 将一个 split 变成一个 元素
mapPartition(func): 对每个 split 执行 func 函数
pipe(command) : 用 streaming 的方式用 command 来过滤 RDD
uniq()        : 返回完全唯一的元素为RDD
cartesian(rdd): 笛卡尔乘积，将两个RDD的元素进行两两组合
cache()       : 返回一个允许缓存的 RDD

一下是对元素是 (key,value) 的 RDD的变换：

mapValue(func): 对value 执行 func
flatMapValue(func): 对 value 执行func, 并合并结果
groupByKey(N) : 把value 按照 key 进行合并，成 key -> [value]
groupBy(func, N): 按照 func(value) 对元素进行合并，成 func(value) -> [value] 
reduceByKey(func, N): 按照 key 合并 value, 并且执行 reduce(func, [value])
combineByKey(agg, N): 按照key合并，并用agg来汇总。
join(rdd)     : 把两个 RDD 按照 key 进行拼装成 key -> (value1,value2)
groupWith(rdds): 按照 key 合并多个 rdds
lookup(key)   : 查找某个key的所有元素，只能查询合并过的RDD

支持的操作如下：

first()       : 返回第一个元素
take(n)       : 返回前 N 个元素为 list
count()       : 返回数据集的大小
collect()     : 返回为 list()
collectAsMap(): 返回为 dict()
reduce(func)  : 对RDD进行 reduce 运算并返回结果。
reduceByKeyToDriver(func): 对 RDD 按照 func(value) 进行 reduce() 操作，并返回给调用者
saveAsTextFile() :保存为文本文件，每个split 保存为一个文件
foreach(func) :对每个元素执行一个函数，无返回值

## 共享变量

Dpark 在运行时，会将RDD以及针对它的操作函数序列化后发送到执行节点去，反序列化并执行。
变换函数所依赖的全局变量，模块和闭包对象等，也都会随着函数一块发送过去。
对于那些无法进行序列化的对象，比如 c 扩展中的函数，需要适当封装一下才可以。比如：

>>> func c_func(v):
        from c_module import func
        return func(v)
>>> rdd.map(c_func)

节点中执行时对某个共享变量的修过，无法传递到调用者，如果需要收集修过，可以通过累加器实现。

这些变换函数依赖的数据在每次小任务计算时都需要传递，如果数据比较大，为了提供性能，可以
使用广播对象来一次性将它传播到每一个结算节点。

### 累加器 Accumulators

累加器只可以收集每次小任务计算时产生的少量数据，可以并行收集，在调用者拿到汇总结果。
用法如下：

>>> acc = dpark.accumulator(0)
>>> dpark.parallelize(range(100)).foreach(lambda x:acc.add(x))
>>> print acc.value
 5050

目前支持数值型和 list 型的累加器。

### 广播常量 Broadcast

广播适用于比较大的数据集，但它不能超过单机的内存限制。用法如下：

>>> v = range(1000000)
>>> bv = dpark.broadcast(v)
>>> bv.value

目前支持共享文件系统和Tree型结构的广播算法。


## 示例

在 examples/ 下有一些用 dpark 实现的例子：

demo.py    演示了 word count, pi 计算，和 迭代算法
cos_c.py   结合C模块来计算条目相似度
weblog.py  格式化每天的日志
pvstat.py  计算每天的PV,UV 等
site_report.py  计算小站的PV,UV,visits, 停留时间等
cinema_report.py 计算各城市的cinema页面的PV
radio_log.py  统计所有用户收听同一首歌的频率分布
shortpath.py  用 Progel 模型来计算最短路径
pagerank.py   用 Progal 模型计算 page rank



