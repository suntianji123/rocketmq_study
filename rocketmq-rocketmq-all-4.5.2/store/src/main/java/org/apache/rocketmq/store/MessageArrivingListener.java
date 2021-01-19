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

package org.apache.rocketmq.store;

import java.util.Map;

/**
 * 消息到达监听器
 */
public interface MessageArrivingListener {

    /**
     * 消息到达处理方法
     * @param topic 消息
     * @param queueId 消息位于的主题消息队列
     * @param logicOffset 消息在消息队列的偏移量
     * @param tagsCode 消息的tag 哈希值
     * @param msgStoreTime 消息的存储到commitlog的时间
     * @param filterBitMap
     * @param properties 消息的属性
     */
    void arriving(String topic, int queueId, long logicOffset, long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties);
}
