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
package org.apache.rocketmq.client.consumer;

import java.util.List;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 啦取消息结果类
 */
public class PullResult {

    /**
     * 拉取消息状态
     */
    private final PullStatus pullStatus;

    /**
     * 下一次拉取消息的偏移量
     */
    private final long nextBeginOffset;

    /**
     * 广播站消费队列消息的最小偏移量
     */
    private final long minOffset;

    /**
     * 广播站消费队列消息的最大偏移量
     */
    private final long maxOffset;

    /**
     * 批量拉取的消息列表
     */
    private List<MessageExt> msgFoundList;

    /**
     * 实例化一个拉取消息结果对象
     * @param pullStatus 拉取消息结果状态
     * @param nextBeginOffset 下一次拉取消息时在消费队列中的偏移量
     * @param minOffset 广播站消息队列消息的最小偏移量
     * @param maxOffset 广播站消息队列消息的最大偏移量
     * @param msgFoundList 批量拉取的消息列表
     */
    public PullResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
        List<MessageExt> msgFoundList) {
        super();
        //设置拉取状态
        this.pullStatus = pullStatus;
        //设置下一次拉取消息的偏移量
        this.nextBeginOffset = nextBeginOffset;
        //设置广播站消费队列的最小偏移量
        this.minOffset = minOffset;
        //设置广播站消息队列的最大偏移量
        this.maxOffset = maxOffset;
        //设置已经拉取到的消息列表
        this.msgFoundList = msgFoundList;
    }

    public PullStatus getPullStatus() {
        return pullStatus;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public List<MessageExt> getMsgFoundList() {
        return msgFoundList;
    }

    public void setMsgFoundList(List<MessageExt> msgFoundList) {
        this.msgFoundList = msgFoundList;
    }

    @Override
    public String toString() {
        return "PullResult [pullStatus=" + pullStatus + ", nextBeginOffset=" + nextBeginOffset
            + ", minOffset=" + minOffset + ", maxOffset=" + maxOffset + ", msgFoundList="
            + (msgFoundList == null ? 0 : msgFoundList.size()) + "]";
    }
}
