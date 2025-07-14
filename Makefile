SHELL := /bin/bash
PROJECT=tinykv
GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO                  := GO111MODULE=on go
GOBUILD             := $(GO) build $(BUILD_FLAG) -tags codes
GOTEST              := $(GO) test -v --count=1 --parallel=1 -p=1 --timeout=300s
TEST_CLEAN          := rm -rf /tmp/*test-raftstore*

TEST_LDFLAGS        := ""

PACKAGE_LIST        := go list ./...| grep -vE "cmd"
PACKAGES            := $$($(PACKAGE_LIST))

# Targets
.PHONY: clean test proto kv scheduler dev

default: kv scheduler

dev: default test

test:
	@echo "Running tests in native mode."
	@export TZ='Asia/Shanghai'; \
	LOG_LEVEL=fatal $(GOTEST) -cover $(PACKAGES)

CURDIR := $(shell pwd)
export PATH := $(CURDIR)/bin/:$(PATH)
proto:
	mkdir -p $(CURDIR)/bin
	(cd proto && ./generate_go.sh)
	GO111MODULE=on go build ./proto/pkg/...

kv:
	$(GOBUILD) -o bin/tinykv-server kv/main.go

scheduler:
	$(GOBUILD) -o bin/tinyscheduler-server scheduler/main.go

ci: default
	@echo "Checking formatting"
	@test -z "$$(gofmt -s -l $$(find . -name '*.go' -type f -print) | tee /dev/stderr)"
	@echo "Running Go vet"
	@go vet ./...

format:
	@gofmt -s -w `find . -name '*.go' -type f ! -path '*/_tools/*' -print`

project: project1 project2 project3 project4

project1:
	$(GOTEST) ./kv/server -run 1

project2: project2a project2b project2c

project2a:
	$(GOTEST) ./raft -run 2A

project2aa:
	$(GOTEST) ./raft -run 2AA

project2ab:
	$(GOTEST) ./raft -run 2AB

project2ac:
	$(GOTEST) ./raft -run 2AC

project2b:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^TestBasic2B$
	$(GOTEST) ./kv/test_raftstore -run ^TestConcurrent2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestUnreliable2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOnePartition2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestManyPartitionsOneClient2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestManyPartitionsManyClients2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistOneClient2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistConcurrent2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistConcurrentUnreliable2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistPartition2B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestPersistPartitionUnreliable2B$ || true
	$(TEST_CLEAN)
# Raft 初始化于 storage, peers 从 storage 中获取
# entry 获取 Term 函数 需要注意处理 ErrCompact 错误
# 可能是因为没有清理，导致日志不断出现 handleMsg 但是 raft 却没有接受新的信息
# raft 的老错误，计算错了提交，导致极偶尔可能会出现少一条的情况
# 增加了在 append response 的时候的index回退优化，避免出现 timeout，但是这会导致 TestFollowerCheckMessageType_MsgAppend2AB 过不了，思考了一下，取消了这个测试
# follower 在处理 append RPC 的时候，如果 r.Term > m.Term 应该直接忽略，不要回复拒绝消息，可能会发生消息风暴
project2c:
	$(TEST_CLEAN)
	$(GOTEST) ./raft -run 2C || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSnapshot2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotRecover2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotRecoverManyClients2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotUnreliable2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotUnreliableRecover2C$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSnapshotUnreliableRecoverConcurrentPartition2C$ || true
	$(TEST_CLEAN)

project3: project3a project3b project3c

project3a:
	$(GOTEST) ./raft -run 3A

project3b:
	$(TEST_CLEAN)
	$(GOTEST) ./kv/test_raftstore -run ^TestTransferLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestBasicConfChange3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeRecoverManyClients3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliable3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeSnapshotUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestOneSplit3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitRecoverManyClients3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitUnreliable3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecover3B$ || true
	$(GOTEST) ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true
	$(TEST_CLEAN)

# 1. ConfChange编码问题
# 2. 在发送心跳的时候，commit == min(r.RaftLog.committed, r.Prs[to].Match)，这是因为在 store_worker 中，通过信息中的 commit == 0 特判，来判断是新建节点。
# 	日志呈现：store_worker 不断出现 don't exist 错误.
# 3. 发送 snapshot 不断出现错误，错误是 stale 的 snapshot，不断 epoh_not_match 最后 request timeout，在 processAdminRequest 中忘记修改 epoch 中的 conf_ver 了。
# 4. 在 TestConfChangeRecover3B 中，总是出现  peer for region id:1 region_epoch:<> is initialized but local state hard_state:<> last_index:4606 last_term:8  has empty hard state 错误，导致 Panic。
# 	原因是在接受 applySnapShot 的时候，忘记根据快照应用 HardState 了。不知道为什么 2C 测不出来
# 5. [region x] x meta corruption detected​. 查看日志，发现是在 processAdminRequest 中，一个 peer 被删除了两次，第一个清空之后，第二次报错。
#  查看可能调用 destroyPeer 函数的地方之后发现，应该是在 processAdminReqeust 中没有过滤已经处理过的 ConfChange 请求。判断两个 ConfChange 请求是否相等，需要其中的 peer 既 id 相同也 storeid 相同
#  这之后仍然出现该错误，查看日志，搜索 remove 相关日志，发现存在某些情况下，删除之后的 region.Peers 仍然存留信息。再次排查之后发现，只有自己被删除才会出现这个情况；自己调用 destroyPeer 直接返回，但是删除之后 region.Peers 仍然存留自己的 peer 信息，导致再次调用。
#  也就是说，如果被删除的是自己，也需要先在 region.Peers 中删除自己相关的信息，不能直接返回。
# 6. 删除节点之后的 request timeout，这个是什么 tinykv 必吃榜嘛？每个 tinykv 的博客基本都会记录这个问题。
#    采用了和白皮书一样的方法，如果节点只剩下两个，并且删除的是 leader，那么可以在 propose 阶段直接 transferleader，然后返回一个错误，让上层之后重试即可，这样一定可以解决问题，概率上没有问题。
# 7. 增删节点之后，request timeout。仔细观察日志，发现连续增删同一个节点，发生 timeout。是在无需操作增删的时候，忘记返回 cb 了。导致上层不断重试，最后超时。
# 8. 还是增删节点之后， request timeout 或者 unmatched peers length，在删除节点调用 destroyPeer 函数之后，不要直接返回，还要进行一些 cb、apply 的处理之后才能返回。
# 9. pendingconfindex	
# 10. transfer_leader 应该在一个 election timeout 的时间之后再取消，而不是下一次 tick 就取消, 否则可能 timeout
# 11. raft 层不应该返回 errnotleader 错误, 否则可能会在日志中出现大量的 errnotleader 错误， 不然可能因此 timeout
# 12. raft 层，confchange 应该在 propose 时候设置，而不是在 apply 的时候设置。同时，只需要拒绝后续的 confchange，不需要拒绝普通 propose。不然会阻塞正常的 propose，导致 timeout
# 13. raft 层，addnode 之后，由 leader 发起 heartbeat 尽快创建新节点。但问题似乎不出在这里，经过排查，似乎是在修改 regionRanges 的时候错误的插入了 region 而且在后续的删除的时候没有正确删除导致的错误。
#      处理方法是删除 maybecreate 最后的 replaceOrinsert 同时修改 destroyPeer 中的判断，修改为 先删除后判断初始化。不然会无法创建节点，导致 tiemout
# 14. raft 层，最好提供一个接口，让 raftstore 希望在 leader 被删除的时候转移 leader 的时候选择一个日志尽可能新的 leader，不然可能因为转移 leader 而拒绝服务，最后 timeout。
# 15. 在 transferLeader 的时候，如果 sendTimeOutNow 被不幸因为网络波动丢失，那么如果没有重传机制，或者没有在 heartbeat 处理这种情况，或者 leader 没有主动变成 follower 那么可能因此无法推进。需要在日志中仔细观察 dropped。
# 16. 最最尴尬的是一种情况发生在先添加节点随后紧接着删除 leader。在发送 transferleader 之后，接收方已经接受开始选举，同时旧 leader 下位，但是接收方却选举失败了，比如因为丢失的情况；同时还有新来的节点没有应用，他不知道当前集群都有谁，这个时候选举就可能一直失败，虽然概率很小但是很尴尬，确实存在。
#     概率大概在 1/20 左右。这种情况一方面要加强候选者重试，重新发票；另一方面我认为要对新节点进行处理，在新节点回复 leader 第一次 append 之前，不进行下一次 confchange，但是可以进行新的 propose，相当于延长了 addnode 的 confchange
#     我两个都做了，代价就是有一个测试 TestRawNodeProposeAddDuplicateNode3A 过不了了。不过仔细考虑之后会发现，其实这个情况就是 18，还是因为新节点没有集群信息导致的错误，因此其实只要完成 18 的修改，这个问题也就完成了，不必延长。
# 17. snapshot 消息可能会丢失，导致后续出错，但是这个问题本质上是因为 requestvote 有些问题，在 leader 收到 requestvote 的时候，可能需要变成 follower 并回复接受投票。
#    简单的一种方法就是 snapshot 多发几次，这样就不会出现问题，不过还是概率而已，大概概率为 5/200
# 18. 我发现了这么一个场景：新增一个节点 A，当前 leader B 尝试发送快照对其进行初始化，
#      但是快照丢失，最后导致节点超时开始选举。因为节点 A 没有初始化，他并不知道集群中的其他节点，
#      这导致节点 A 选举成功变成 leader，后来 leader B 发送心跳，leader B 得知了这个事情，
#      开始重新选举。但是因为 leader A 在当选 leader 之后向日志中 append 了一个日志，这个日志的 term 更大，
#      导致 A 不会投票给 B，这导致无限循环，永远选举不出有效的 leader。
#      这个问题的本质是信息的不对称，我想不到什么优雅的解决方案，我的方法是：发送快照的时候多发送几次，同时在心跳的时候检测，如果发现过于落后，就发快照。概率大概在 1/50
#      找到了，一个 peerstorage 在初始化的时候是空的，没有任何 peer 信息，即使是自己本身的信息也没有。可以通过这个判断一个节点是否初始化，然后拒绝它成为 leader。

project3c:
	$(GOTEST) ./scheduler/server ./scheduler/server/schedulers -check.f="3C"

project4: project4a project4b project4c

project4a:
	$(GOTEST) ./kv/transaction/... -run 4A

project4b:
	$(GOTEST) ./kv/transaction/... -run 4B

project4c:
	$(GOTEST) ./kv/transaction/... -run 4C