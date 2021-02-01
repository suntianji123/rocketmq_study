/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.store;

import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;

public abstract class DLedgerStore {

    public MemberState getMemberState() {
        return null;
    }

    /**
     * 向leader节点的commitlog中添加消息
     * @param entry 消息实体
     * @return
     */
    public abstract DLedgerEntry appendAsLeader(DLedgerEntry entry);

    /**
     * 同步leader节点推送的消息实体
     * @param entry 消息实体
     * @param leaderTerm 当前轮次
     * @param leaderId leader节点id
     * @return
     */
    public abstract DLedgerEntry appendAsFollower(DLedgerEntry entry, long leaderTerm, String leaderId);

    /**
     * 根据index获取某个消息实体
     * @param index 消息index值
     * @return
     */
    public abstract DLedgerEntry get(Long index);

    public abstract long getCommittedIndex();

    public void updateCommittedIndex(long term, long committedIndex) {

    }

    public abstract long getLedgerEndTerm();

    public abstract long getLedgerEndIndex();

    public abstract long getLedgerBeginIndex();

    /**
     * 更新节点状态机 最后一条消息的索引位置 轮次
     */
    protected void updateLedgerEndIndexAndTerm() {
        if (getMemberState() != null) {
            getMemberState().updateLedgerIndexAndTerm(getLedgerEndIndex(), getLedgerEndTerm());
        }
    }

    public void flush() {

    }

    public long truncate(DLedgerEntry entry, long leaderTerm, String leaderId) {
        return -1;
    }

    public void startup() {

    }

    public void shutdown() {

    }
}
