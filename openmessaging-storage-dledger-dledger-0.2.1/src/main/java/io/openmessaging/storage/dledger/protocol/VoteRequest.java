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

package io.openmessaging.storage.dledger.protocol;

/**
 * 拉票请求类
 */
public class VoteRequest extends RequestOrResponse {

    /**
     * 发起请求节点的最后一条消息的index值
     */
    private long ledgerEndIndex = -1;

    /**
     * 发起请求节点的最后一条消息的轮次
     */
    private long ledgerEndTerm = -1;

    public long getLedgerEndIndex() {
        return ledgerEndIndex;
    }

    public void setLedgerEndIndex(long ledgerEndIndex) {
        this.ledgerEndIndex = ledgerEndIndex;
    }

    public long getLedgerEndTerm() {
        return ledgerEndTerm;
    }

    public void setLedgerEndTerm(long ledgerEndTerm) {
        this.ledgerEndTerm = ledgerEndTerm;
    }
}
