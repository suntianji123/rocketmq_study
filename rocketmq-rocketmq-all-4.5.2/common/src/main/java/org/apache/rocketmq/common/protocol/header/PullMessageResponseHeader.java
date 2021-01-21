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

/**
 * $Id: PullMessageResponseHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

/**
 * 从广播站拉取消息的响应头类
 */
public class PullMessageResponseHeader implements CommandCustomHeader {

    /**
     * 建议下一次从消费队列批量拉取消息时
     * 如果下一次从commiltlog拉取的消息的起始偏移量在内存中存在 选择主站
     * 如果下一次从commitlog拉取的消息的起始偏移量在内存中不存在 选择id为1的从站
     */
    @CFNotNull
    private Long suggestWhichBrokerId;

    /**
     * 下一次从消费队列拉取消息的偏移量
     */
    @CFNotNull
    private Long nextBeginOffset;

    /**
     * 消费队列消息偏移量的最小值
     */
    @CFNotNull
    private Long minOffset;

    /**
     * 消费队列消息偏移量的最大值
     */
    @CFNotNull
    private Long maxOffset;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(Long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public Long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(Long minOffset) {
        this.minOffset = minOffset;
    }

    public Long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(Long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public Long getSuggestWhichBrokerId() {
        return suggestWhichBrokerId;
    }

    public void setSuggestWhichBrokerId(Long suggestWhichBrokerId) {
        this.suggestWhichBrokerId = suggestWhichBrokerId;
    }
}
