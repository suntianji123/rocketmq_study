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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 生产者生产的同一主题的消息 保存于多个广播站的多个消息队列 分散保存
 * 当前进程的消费者订阅了这个主题的消息 获取消息的来源必须是从指定的消息队列列表来获取
 * 分配主题消息队列策略类
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();


    /**
     * 分配方法 指定进程分配的主题消息队列列表
     * @param consumerGroup 消费者组名
     * @param currentCID 当前消费者进程远程客户端id
     * @param mqAll 主题对应的主题消息队列列表
     * @param cidAll 消费者集群的远程客户端id列表
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        //当前消费者进程的远程客户端id 不能为空
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }

        //主题对应的主题消息队列不能为null
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }

        //消费者集群的远程客户端id列表不能为null
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        //消费者集群的远程客户端id列表必须包含当前的远程客户端id
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        //获取当前消费者进程id在消费者集群的客户端id列表中的下标
        int index = cidAll.indexOf(currentCID);

        //均分 获取多余的
        int mod = mqAll.size() % cidAll.size();

        //获取分配的大小 （index在前面的多分一点 在后面的少分一点）
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        //分配的主题消息队列的起始下标
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        //可以分配的消息队列的数量
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }

        //结果的主题消息队列
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
