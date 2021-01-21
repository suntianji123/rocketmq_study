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
package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 拉取消息结果扩展
 */
public class PullResultExt extends PullResult {

    /**
     * 建议下次拉取消息的集群广播站id
     */
    private final long suggestWhichBrokerId;

    /**
     * 拉取到的批量消息的字节数组
     */
    private byte[] messageBinary;

    /**
     * 实例化一个拉取消息结果的扩展对象
     * @param pullStatus 拉取消息结果
     * @param nextBeginOffset 下一次拉取消息的起始偏移量
     * @param minOffset 广播站消费队列的消息的最小偏移量
     * @param maxOffset 广播站消息队列的消息的最大偏移量
     * @param msgFoundList 拉取到的批量消息
     * @param suggestWhichBrokerId 下一次拉取消息时 建议使用的广播站id
     * @param messageBinary 批量拉取的消息的字节数组
     */
    public PullResultExt(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
        List<MessageExt> msgFoundList, final long suggestWhichBrokerId, final byte[] messageBinary) {
        super(pullStatus, nextBeginOffset, minOffset, maxOffset, msgFoundList);
        this.suggestWhichBrokerId = suggestWhichBrokerId;
        this.messageBinary = messageBinary;
    }

    public byte[] getMessageBinary() {
        return messageBinary;
    }

    public void setMessageBinary(byte[] messageBinary) {
        this.messageBinary = messageBinary;
    }

    public long getSuggestWhichBrokerId() {
        return suggestWhichBrokerId;
    }
}
