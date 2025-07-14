// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	// SuitableStore 是那些满足 IsUp() 且 DownTime() 时间小于 MaxStoreDownTime 的 store。
	suitableStores := make([]*core.StoreInfo, 0)

	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}

	if len(suitableStores) <= 1 {
		return nil
	}

	// 选出 suitableStores，并按照 regionSize 进行排序。
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})

	// 开始遍历 suitableStores，从 regionSize 最大的开始遍历，
	// 依次调用 GetPendingRegionsWithLock()，GetFollowersWithLock() 和 GetLeadersWithLock()。
	// 直到找到一个目标 region。如果实在找不到目标 region，直接放弃本次操作。
	region := (*core.RegionInfo)(nil)
	for i := 0; i < len(suitableStores); i++ {
		cluster.GetPendingRegionsWithLock(suitableStores[i].GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil, nil)
		})

		if region != nil {
			break
		}

		cluster.GetFollowersWithLock(suitableStores[i].GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil, nil)
		})

		if region != nil {
			break
		}

		cluster.GetLeadersWithLock(suitableStores[i].GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil, nil)
		})

		if region != nil {
			break
		}
	}

	// 判断目标 region 的 store 数量，如果小于 cluster.GetMaxReplicas()，直接放弃本次操作。
	if region == nil || len(region.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}

	target := (*core.StoreInfo)(nil)
	source := suitableStores[0]

	for i :=len(suitableStores)-1; i >= 0; i-- {
		if region.GetStorePeer(suitableStores[i].GetID()) == nil {
			target = suitableStores[i]
			break
		}
	}

	if target == nil {
		return nil
	}

	diff := source.GetRegionSize() - target.GetRegionSize()

	if diff <= 2*region.GetApproximateSize() {
		return nil
	}

	newPeer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		panic(err)
	}

	oper, err := operator.CreateMovePeerOperator("balance_region", cluster, region, operator.OpBalance, source.GetID(), target.GetID(), newPeer.GetId())
	if err != nil {
		panic(err)
	}

	return oper
}
