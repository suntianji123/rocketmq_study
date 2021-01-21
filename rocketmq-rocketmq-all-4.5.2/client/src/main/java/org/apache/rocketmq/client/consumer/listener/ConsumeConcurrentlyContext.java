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
package org.apache.rocketmq.client.consumer.listener;

import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 即时消费上下文类
 */
public class ConsumeConcurrentlyContext {

    /**
     * 主题消息队列
     */
    private final MessageQueue messageQueue;

    /* *
    *邮件消耗重试策略<br>
     * -1，不重试，直接放入DLQ <br>
      *0，广播站控制重试频率<br>
       *0，客户端控制重试频率
    */
    private int delayLevelWhenNextConsume = 0;

    /**
     * 最后一个成功被消费者消费的消息在消费请求的消息列表中的下标
     */
    private int ackIndex = Integer.MAX_VALUE;

    /**
     * 实例化一个即时消息队列对象
     * @param messageQueue
     */
    public ConsumeConcurrentlyContext(MessageQueue messageQueue) {
        //设置主题消息队列
        this.messageQueue = messageQueue;
    }

    public int getDelayLevelWhenNextConsume() {
        return delayLevelWhenNextConsume;
    }

    public void setDelayLevelWhenNextConsume(int delayLevelWhenNextConsume) {
        this.delayLevelWhenNextConsume = delayLevelWhenNextConsume;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public int getAckIndex() {
        return ackIndex;
    }

    public void setAckIndex(int ackIndex) {
        this.ackIndex = ackIndex;
    }
}
