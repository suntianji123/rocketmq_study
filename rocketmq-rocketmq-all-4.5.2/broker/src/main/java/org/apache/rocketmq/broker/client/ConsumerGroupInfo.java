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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * 消费者信息类
 */
public class ConsumerGroupInfo {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 消费者组名
     */
    private final String groupName;
    private final ConcurrentMap<String/* Topic */, SubscriptionData> subscriptionTable =
        new ConcurrentHashMap<String, SubscriptionData>();

    /**
     * 所有使用这个名字的消费者集群的channel
     */
    private final ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
        new ConcurrentHashMap<Channel, ClientChannelInfo>(16);
    /**
     * 消费者获取消息的方式枚举
     */
    private volatile ConsumeType consumeType;

    /**
     * 消息模型
     */
    private volatile MessageModel messageModel;

    /**
     * 消息者从哪开始消费
     */
    private volatile ConsumeFromWhere consumeFromWhere;

    /**
     * 最后一次更新时间
     */
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    /**
     * 实例化一个消费者信息类
     * @param groupName 消息者组名
     * @param consumeType 消费者获取消息的方式枚举 推送 | 主动拉取
     * @param messageModel 消息模型 集群 |
     * @param consumeFromWhere 消息从哪开始消费
     */
    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel,
        ConsumeFromWhere consumeFromWhere) {
        //设置消费者组名
        this.groupName = groupName;
        //设置获取消息的方式
        this.consumeType = consumeType;
        //设置消息模型
        this.messageModel = messageModel;
        //设置从哪开始消费
        this.consumeFromWhere = consumeFromWhere;
    }

    public ClientChannelInfo findChannel(final String clientId) {
        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> next = it.next();
            if (next.getValue().getClientId().equals(clientId)) {
                return next.getValue();
            }
        }

        return null;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionTable() {
        return subscriptionTable;
    }

    public ConcurrentMap<Channel, ClientChannelInfo> getChannelInfoTable() {
        return channelInfoTable;
    }

    public List<Channel> getAllChannel() {
        List<Channel> result = new ArrayList<>();

        result.addAll(this.channelInfoTable.keySet());

        return result;
    }

    public List<String> getAllClientId() {
        List<String> result = new ArrayList<>();

        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> entry = it.next();
            ClientChannelInfo clientChannelInfo = entry.getValue();
            result.add(clientChannelInfo.getClientId());
        }

        return result;
    }

    public void unregisterChannel(final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo old = this.channelInfoTable.remove(clientChannelInfo.getChannel());
        if (old != null) {
            log.info("unregister a consumer[{}] from consumerGroupInfo {}", this.groupName, old.toString());
        }
    }

    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        final ClientChannelInfo info = this.channelInfoTable.remove(channel);
        if (info != null) {
            log.warn(
                "NETTY EVENT: remove not active channel[{}] from ConsumerGroupInfo groupChannelTable, consumer group: {}",
                info.toString(), groupName);
            return true;
        }

        return false;
    }

    /**
     * 更新消费者的通道
     * @param infoNew 消费者channel通道信息
     * @param consumeType 消息者获取消息的方式
     * @param messageModel 消息模型
     * @param consumeFromWhere 消费者从哪开始消费
     * @return
     */
    public boolean updateChannel(final ClientChannelInfo infoNew, ConsumeType consumeType,
        MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        boolean updated = false;
        //设置获取消息的方式
        this.consumeType = consumeType;
        //设置消息模型
        this.messageModel = messageModel;
        //设置从哪开始消费
        this.consumeFromWhere = consumeFromWhere;

        //之前不存在channelinfo
        ClientChannelInfo infoOld = this.channelInfoTable.get(infoNew.getChannel());
        if (null == infoOld) {
            ClientChannelInfo prev = this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            if (null == prev) {
                log.info("new consumer connected, group: {} {} {} channel: {}", this.groupName, consumeType,
                    messageModel, infoNew.toString());
                updated = true;
            }

            infoOld = infoNew;
        } else {

            //channel的clientid变更
            if (!infoOld.getClientId().equals(infoNew.getClientId())) {
                log.error("[BUG] consumer channel exist in broker, but clientId not equal. GROUP: {} OLD: {} NEW: {} ",
                    this.groupName,
                    infoOld.toString(),
                    infoNew.toString());
                this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            }
        }

        //设置最后一次更新时间
        this.lastUpdateTimestamp = System.currentTimeMillis();
        //设置channelinfo的最后一次更新时间
        infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);



        return updated;
    }

    /**
     * 更新消费者订阅主题列表
     * @param subList 订阅主题列表
     * @return
     */
    public boolean updateSubscription(final Set<SubscriptionData> subList) {
        boolean updated = false;

        for (SubscriptionData sub : subList) {//遍历每一个主题
            //获取老的订阅信息
            SubscriptionData old = this.subscriptionTable.get(sub.getTopic());
            if (old == null) {//之前不存在
                //将新的存入
                SubscriptionData prev = this.subscriptionTable.putIfAbsent(sub.getTopic(), sub);
                if (null == prev) {
                    updated = true;
                    log.info("subscription changed, add new topic, group: {} {}",
                        this.groupName,
                        sub.toString());
                }
            } else if (sub.getSubVersion() > old.getSubVersion()) {//订阅版本法神该变更
                if (this.consumeType == ConsumeType.CONSUME_PASSIVELY) {
                    log.info("subscription changed, group: {} OLD: {} NEW: {}",
                        this.groupName,
                        old.toString(),
                        sub.toString()
                    );
                }

                this.subscriptionTable.put(sub.getTopic(), sub);
            }
        }

        //删除新的已经删除的订阅
        Iterator<Entry<String, SubscriptionData>> it = this.subscriptionTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionData> next = it.next();
            String oldTopic = next.getKey();

            boolean exist = false;
            for (SubscriptionData sub : subList) {
                if (sub.getTopic().equals(oldTopic)) {
                    exist = true;
                    break;
                }
            }

            if (!exist) {
                log.warn("subscription changed, group: {} remove topic {} {}",
                    this.groupName,
                    oldTopic,
                    next.getValue().toString()
                );

                it.remove();
                updated = true;
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();

        return updated;
    }

    public Set<String> getSubscribeTopics() {
        return subscriptionTable.keySet();
    }

    public SubscriptionData findSubscriptionData(final String topic) {
        return this.subscriptionTable.get(topic);
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public String getGroupName() {
        return groupName;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }
}
