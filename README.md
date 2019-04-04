# rust learn
### kv_server_week1
1. 支持 Put，Get，Delete 和 Scan 操作；
2. 没有持久性的kv server；
3. kv使用BtreeMap存储在内存；
4. server和client通信使用grpcio。
### kv_server_week2
在`kv_server_week1`的基础上添加：
简单实现以下功能：
1. 持久性：单纯写log，recover时读log文件;
2. 并发：使用读写锁RwLock实现，写log采用batch实现，默认为1(1条kv即flush)。


