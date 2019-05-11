# rust learn
参加PingCAP公司的培训计划第二期，为期两个月。

这里包含了写的一些东西，语言：rust语言。
其中raft，kv_raft，percolator都是单个实验的，里面包含了各个阶段的代码，
当然这里面肯定有很多错误的，在后面阶段有改正。

dss则是最后的代码，包含raft、kv_raft、percolator，原架构在：https://github.com/pingcap/talent-plan


## kv_server_week1
1. 支持 Put，Get，Delete 和 Scan 操作；
2. 没有持久性的kv server；
3. kv使用BtreeMap存储在内存；
4. server和client通信使用grpcio。
## kv_server_week2
在`kv_server_week1`的基础上添加：
简单实现以下功能：
1. 持久性：单纯写log，recover时读log文件;
2. 并发：使用读写锁RwLock实现，写log采用batch实现，默认为1(1条kv即flush)。


## raft_week3
是raft的实现，框架是rust语言，根据go语言翻译过来的。

实验指导：https://pdos.csail.mit.edu/6.824/labs/lab-raft.html

## kv_raft_week4
在raft的基础上增加kv_server。
实验指导：https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html

## percolator
percolator，两阶段提交

## dss
最终的代码,包含raft、kv_raft、percolator。