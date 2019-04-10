# rust for raft lab_2A
实现领导人选举和心跳。

## 测试结果
```
running 2 tests
test raft::tests::test_initial_election_2a ... 
[2019-04-10T12:44:24Z INFO  labs6824::raft::config] Test (2A): initial election ...
[2019-04-10T12:44:27Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-10T12:44:27Z INFO  labs6824::raft::config]   3.07488606s  3 256 0
ok
test raft::tests::test_reelection_2a ... 
[2019-04-10T12:44:28Z INFO  labs6824::raft::config] Test (2A): election after network failure ...
[2019-04-10T12:44:32Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-10T12:44:32Z INFO  labs6824::raft::config]   4.497245494s  3 456 0
ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 37 filtered out
```


## 遇到的问题
1. 当编译的时候遇到：
```
error[E0554]: #![feature] may not be used on the stable release channel
 --> labrpc/src/lib.rs:4:1
  |
4 | #![feature(test)]
  | ^^^^^^^^^^^^^^^^^

error: aborting due to previous error

For more information about this error, try `rustc --explain E0554`.
error: Could not compile `labrpc`.

```
编译labrpc需要安装clippy，而该工具只能在nightly模式下使用

解决：
```
> rustup override set nightly  # 手动切换到nightly模式
> cargo install --git https://github.com/rust-lang/rust-clippy/ --force clippy  # 安装clippy

```
2. 写代码时遇到的问题：

使用Raft结构里面的client发送请求时，一定要clone客户端出来然后创建线程运行。

因为client请求返回Err时，由于wait()一般需要等待好几秒，如果调用raft里面的接口，会一直占用raft的锁，极容易导致死锁。