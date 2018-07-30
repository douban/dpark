=========
Dpark FAQ
=========

FAQ
===

1. 为什么我指定了'-m mesos'参数，但dpark似乎还是在单机模式运行？
---------------------------------------------------------------------

因为dpark的参数解析器看到第一个不能识别的参数就会放弃对后面参数的解析，所以‘-m mesos’这类的dpark参数请放在参数列表的最前面。另外dpark之所以不解析第一个不能识别的参数后面的参数，是为了支持‘drun ls -al’这类命令行的解析。

2. 为什么我的用dpark写的文件会损坏？
------------------------------------------

因为dpark并不保证每个task同时只有一个在运行，特别是当某个task执行的比这个 taskset 的其他task慢很多，dpark会尝试重新提交这个task，这样就会有相同的task在同时运行。此时当这个task有对共享存储的文件的写操作时，文件就很可能损坏。解决办法是在写文件时先写到一个文件名随机（或者用机器名+PID生成）的临时文件中，最后再将临时文件rename成最终文件

3. 为什么我在集群上的任务都会卡住或者失败？
--------------------------------------------------

因为你在计算集群上没有帐号，所以无法以自己的身份来提交计算任务，所以请用公共帐号mesos来提交，具体使用方式是sudo -u mesos 后面加上你的计算脚本命令行。 另一种解决方法是在 `helpdesk <http://sysadmin.douban.com/helpdesk/>`_ 上申请 dpark_members 机群的权限。

4. saveAsTextFile会在目录中产生许许多多的小文件，能让文件变少点么？
------------------------------------------------------------------------

在saveAsTextFile之前用mergeSplit来减少输出文件数目，例如mergeSplit(3)会把原来每3个小文件合并为1个文件

5. parallelize 使用陷阱
--------------------------

``parallelize(list_foo, num_split)`` 方式对 ``list_foo`` 进行分块时，会先将其传输到master节点，当 ``list_foo`` 非常大(say 100 MB)时，过长的网络传输时间可能会引发如下ERROR或WARNING:
``Lost Task`` ,  ``use too much time in slaveOffer`` , ``Connection dropped`` , ``Session has expired`` ，这时不管怎么调节 ``num_split`` 都是没有用的。一种可能的解决办法是将list_foo写入文本文件，最后以 ``textFile`` 读出进行分块。

6. 很多 “[WARNING] [dpark.taskset] Lost Task”
-----------------------------------------------

原因可能因为你在计算集群上没有账号，解决方法参考问题 3。

7. 一些奇怪的模块依赖错误
--------------------------------

原因可能是因为你的 DPark 脚本所在的文件夹或者你的 PYTHONPATH 路径上，有一些你自己编写的模块和标准库里的模块重名了 (e.g. email.py)。下面是个演示的例子，把它们放在同一文件夹下，然后执行 `python wc.py -m mesos`

::

     # email.py
     import traceback
     traceback.print_stack()


     # wc.py
     from dpark import DparkContext
     import random

     ctx = DparkContext()
     rdd = ctx.parallelize([random.randint(0, 10) for _ in range(100)], 5)
     print rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()

8. 让人挠头的幽灵文件
---------------------

你有一个通过 dpark saveAsTextFile 的输出，然后用一个其他的程序读取这个输出目录下的文件，结果程序奇怪的崩溃了。你发现是文件的输入格式不符合预期。你检查了你的 dpark 程序，但并没有什么收获。还尝试写了一个 dpark 程序想看看这些不符合预期的输入行，但是没有找到。正当你百思不得其解之际，你想起了 dpark 输出目录下的 tmp 文件。对的，就是他们。

dpark 同一个 task 会同时执行，但并不能保证这些临时输出的 tmp 文件会被清除掉，而 textFile 会主动忽略隐藏文件的。所以下次你可以主动清理一下这些 tmp 文件，或者在程序里做一些读入检查。不过你看到这一条的时候也许已经知道为什么了。


