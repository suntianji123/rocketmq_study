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
package org.apache.rocketmq.client.consumer.store;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 远程广播站存
 */
public class RemoteBrokerOffsetStore implements OffsetStore {
    private final static InternalLogger log = ClientLogger.getLog();

    /**
     * mqclientinstance对象
     */
    private final MQClientInstance mQClientFactory;

    /**
     * 消费者组名
     */
    private final String groupName;

    /**
     * 消费者组的主题消息队列对应的偏移量
     */
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>();

    /**
     * 实例化一个远程的主题消息队列消费偏移量存储
     * @param mQClientFactory 访问远程serve的mqclientinstance对象
     * @param groupName 消费者组名
     */
    public RemoteBrokerOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        //设置mqclientinstance对戏
        this.mQClientFactory = mQClientFactory;
        //设置生产者组名
        this.groupName = groupName;
    }

    /**
     * 加载主题消息队列的偏移量
     */
    @Override
    public void load() {
    }

    /**
     * 更新某个主题消息队列的消费偏移量
     * @param mq 主题消息队列
     * @param offset 消费偏移量
     * @param increaseOnly 是否只是增加
     */
    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            AtomicLong offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    offsetOld.set(offset);
                }
            }
        }
    }

    /**
     * 获取某个主题消息队列对应的偏移量
     * @param mq 主题消息队列
     * @param type 读偏移量
     * @return
     */
    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                case READ_FROM_STORE: {//READ_FROM_STORE
                    try {
                        //从广播站获取主题队列的消费偏移量
                        long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                        AtomicLong offset = new AtomicLong(brokerOffset);
                        this.updateOffset(mq, offset.get(), false);
                        return brokerOffset;
                    }
                    // No offset in broker
                    catch (MQBrokerException e) {
                        return -1;
                    }
                    //Other exceptions
                    catch (Exception e) {
                        log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                        return -2;
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;

        final HashSet<MessageQueue> unusedMQ = new HashSet<MessageQueue>();
        if (!mqs.isEmpty()) {
            for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                AtomicLong offset = entry.getValue();
                if (offset != null) {
                    if (mqs.contains(mq)) {
                        try {
                            this.updateConsumeOffsetToBroker(mq, offset.get());
                            log.info("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                                this.groupName,
                                this.mQClientFactory.getClientId(),
                                mq,
                                offset.get());
                        } catch (Exception e) {
                            log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
                        }
                    } else {
                        unusedMQ.add(mq);
                    }
                }
            }
        }

        if (!unusedMQ.isEmpty()) {
            for (MessageQueue mq : unusedMQ) {
                this.offsetTable.remove(mq);
                log.info("remove unused mq, {}, {}", mq, this.groupName);
            }
        }
    }

    /**
     * 将本地的主题消息队列的消费偏移量同步到远程广播站
     * @param mq 主题消息队列
     */
    @Override
    public void persist(MessageQueue mq) {
        //获取本地主题消息队列的消费偏移量
        AtomicLong offset = this.offsetTable.get(mq);
        if (offset != null) {
            try {
                //将本地的主题消息队列的消费偏移量同步到远程广播站
                this.updateConsumeOffsetToBroker(mq, offset.get());
                log.info("[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                    this.groupName,
                    this.mQClientFactory.getClientId(),
                    mq,
                    offset.get());
            } catch (Exception e) {
                log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
            }
        }
    }

    /**
     * 删除某个主题消息队列偏移统计
     * @param mq
     */
    public void removeOffset(MessageQueue mq) {
        if (mq != null) {
            this.offsetTable.remove(mq);
            log.info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq,
                offsetTable.size());
        }
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());
        }
        return cloneOffsetTable;
    }


    /**
     * 将本地的主题消息队列的消费偏移量同步到远程广播站
     * @param mq 消息队列
     * @param offset 消费偏移量
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        updateConsumeOffsetToBroker(mq, offset, true);
    }


    /**
     * 将本地的主题消费队列的消费偏移量同步到远程广播站
     * @param mq 主题消息队列
     * @param offset 消费偏移量
     * @param isOneway 是否是需要广播站回复的请求
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    @Override
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {

        //获取消息队列广播站主站地址
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            //从中心服务器更新一次广播站列表
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            //更新某个消息队列的消费偏移量请求头
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            //设置主题
            requestHeader.setTopic(mq.getTopic());
            //设置消费者组名
            requestHeader.setConsumerGroup(this.groupName);
            //设置主题消息队列编号
            requestHeader.setQueueId(mq.getQueueId());
            //设置新的偏移量
            requestHeader.setCommitOffset(offset);

            if (isOneway) {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffsetOneway(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            } else {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffset(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            }
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }

    /**
     * 寻找消费者的某个主题消息队列偏移量
     * @param mq 主题消息队列
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        //寻找一个广播站
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {//如果没有寻找到广播站  从中心服务更新一次
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        //寻找到了广播站
        if (findBrokerResult != null) {
            //实例化一个消费者偏移量的请求头
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            //设置主题
            requestHeader.setTopic(mq.getTopic());
            //设置消费者组名
            requestHeader.setConsumerGroup(this.groupName);
            //设置主题队列编号
            requestHeader.setQueueId(mq.getQueueId());

            return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(
                findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }
}
