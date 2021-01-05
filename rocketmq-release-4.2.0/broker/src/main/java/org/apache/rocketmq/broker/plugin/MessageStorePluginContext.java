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

package org.apache.rocketmq.broker.plugin;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * 消息存储插件上下文类
 */
public class MessageStorePluginContext {
    /**
     * 消息存储配置
     */
    private MessageStoreConfig messageStoreConfig;
    /**
     * 消息统计管理器
     */
    private BrokerStatsManager brokerStatsManager;

    /**
     * 消息到达之后的监听器
     */
    private MessageArrivingListener messageArrivingListener;

    /**
     * 广播站配置
     */
    private BrokerConfig brokerConfig;

    /**
     * 实例化一个消息存储上下文对象
     * @param messageStoreConfig 消息存储配置
     * @param brokerStatsManager 消息统计管理器
     * @param messageArrivingListener 消息到达之后的监听器
     * @param brokerConfig 广播站配置对象
     */
    public MessageStorePluginContext(MessageStoreConfig messageStoreConfig,
        BrokerStatsManager brokerStatsManager, MessageArrivingListener messageArrivingListener,
        BrokerConfig brokerConfig) {
        super();

        //设置消息存储配置爱对象
        this.messageStoreConfig = messageStoreConfig;

        //设置消息统计管理器
        this.brokerStatsManager = brokerStatsManager;

        //设置消息到达之后的监听器
        this.messageArrivingListener = messageArrivingListener;

        //设置消息配置
        this.brokerConfig = brokerConfig;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public MessageArrivingListener getMessageArrivingListener() {
        return messageArrivingListener;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

}
