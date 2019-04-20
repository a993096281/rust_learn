# rust for raft lab_2C
实现持久化和不可靠网络传输测试。
可通过lab_2a,lab_2b和lab_2c。
不可靠网络测试：test_figure_8_unreliable_2c并不能保证完全通过，因为测试先是累积小于1000个log，

然后恢复，需要保证10秒内能提交一条新日志，大约2秒给一个新leader发送新日志，意味着只有4.5次机会，而不可靠网络传输，

导致某个节点超时非常正常，结果又得重新选举，重新选举意味着很大可能性浪费了一次机会，而且日志比较多，同步可能要花费很多时间，

这增大了节点超时的概率。

## 测试结果
lab_2a
```
running 2 tests
test raft::tests::test_initial_election_2a ... 
[2019-04-20T06:24:47Z INFO  labs6824::raft::config] Test (2A): initial election ...
[2019-04-20T06:24:51Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:24:51Z INFO  labs6824::raft::config]   3.098044057s  3 62 0
ok
test raft::tests::test_reelection_2a ... 
[2019-04-20T06:24:53Z INFO  labs6824::raft::config] Test (2A): election after network failure ...
[2019-04-20T06:24:57Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:24:57Z INFO  labs6824::raft::config]   4.537993445s  3 144 0
ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 37 filtered out

```
lab_2b
```
running 7 tests
test raft::tests::test_backup_2b ... 
[2019-04-20T06:25:29Z INFO  labs6824::raft::config] Test (2B): leader backs up quickly over incorrect follower logs ...
[2019-04-20T06:25:56Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:25:56Z INFO  labs6824::raft::config]   26.383804623s  5 2324 102
ok
test raft::tests::test_basic_agree_2b ... 
[2019-04-20T06:25:59Z INFO  labs6824::raft::config] Test (2B): basic agreement ...
[2019-04-20T06:26:00Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:26:00Z INFO  labs6824::raft::config]   836.271209ms  5 32 3
ok
test raft::tests::test_concurrent_starts_2b ... 
[2019-04-20T06:26:03Z INFO  labs6824::raft::config] Test (2B): concurrent start()s ...
[2019-04-20T06:26:04Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:26:04Z INFO  labs6824::raft::config]   611.278692ms  3 12 6
ok
test raft::tests::test_count_2b ... 
[2019-04-20T06:26:06Z INFO  labs6824::raft::config] Test (2B): RPC counts aren't too high ...
[2019-04-20T06:26:08Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:26:08Z INFO  labs6824::raft::config]   2.293413086s  3 44 12
ok
test raft::tests::test_fail_agree_2b ... 
[2019-04-20T06:26:11Z INFO  labs6824::raft::config] Test (2B): agreement despite follower disconnection ...
[2019-04-20T06:26:16Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:26:16Z INFO  labs6824::raft::config]   5.926549323s  3 132 8
ok
test raft::tests::test_fail_no_agree_2b ... 
[2019-04-20T06:26:19Z INFO  labs6824::raft::config] Test (2B): no agreement if too many followers disconnect ...
[2019-04-20T06:26:22Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:26:22Z INFO  labs6824::raft::config]   3.601811516s  5 236 3
ok
test raft::tests::test_rejoin_2b ... 
[2019-04-20T06:26:26Z INFO  labs6824::raft::config] Test (2B): rejoin of partitioned leader ...
[2019-04-20T06:26:32Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:26:32Z INFO  labs6824::raft::config]   6.137733348s  3 192 4
ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 32 filtered out

```
lab_2c
```
running 8 tests
test raft::tests::test_figure_8_2c ... 
[2019-04-20T06:19:28Z INFO  labs6824::raft::config] Test (2C): Figure 8 ...
[2019-04-20T06:20:31Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:20:31Z INFO  labs6824::raft::config]   62.991630551s  5 1420 24
ok
test raft::tests::test_figure_8_unreliable_2c ... 
[2019-04-20T06:20:34Z INFO  labs6824::raft::config] Test (2C): Figure 8 (unreliable) ...
[2019-04-20T06:21:12Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:21:12Z INFO  labs6824::raft::config]   37.977653049s  5 3664 69
ok
test raft::tests::test_persist1_2c ... 
[2019-04-20T06:21:16Z INFO  labs6824::raft::config] Test (2C): basic persistence ...
[2019-04-20T06:21:23Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:21:23Z INFO  labs6824::raft::config]   7.256398925s  3 158 6
ok
test raft::tests::test_persist2_2c ... 
[2019-04-20T06:21:25Z INFO  labs6824::raft::config] Test (2C): more persistence ...
[2019-04-20T06:22:02Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:22:02Z INFO  labs6824::raft::config]   36.945130288s  5 2404 19
ok
test raft::tests::test_persist3_2c ... 
[2019-04-20T06:22:06Z INFO  labs6824::raft::config] Test (2C): partitioned leader and one follower crash, leader restarts ...
[2019-04-20T06:22:10Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:22:10Z INFO  labs6824::raft::config]   4.110824791s  3 60 4
ok
test raft::tests::test_reliable_churn_2c ... 
[2019-04-20T06:22:12Z INFO  labs6824::raft::config] Test (2C): churn ...
[2019-04-20T06:22:30Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:22:30Z INFO  labs6824::raft::config]   17.899468081s  5 1172 216
ok
test raft::tests::test_unreliable_agree_2c ... 
[2019-04-20T06:22:33Z INFO  labs6824::raft::config] Test (2C): unreliable agreement ...
[2019-04-20T06:22:47Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:22:47Z INFO  labs6824::raft::config]   13.348495755s  5 564 251
ok
test raft::tests::test_unreliable_churn_2c ... 
[2019-04-20T06:22:50Z INFO  labs6824::raft::config] Test (2C): unreliable churn ...
[2019-04-20T06:23:09Z INFO  labs6824::raft::config]   ... Passed --
[2019-04-20T06:23:09Z INFO  labs6824::raft::config]   19.118892066s  5 896 169
ok

test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 31 filtered out

```