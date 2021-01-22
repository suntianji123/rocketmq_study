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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class PullAPIWrapper {
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String consumerGroup;
    private final boolean unitMode;

    /**
     * 消费者从主题消息队列拉取消息时 建议使用的广播站id
     */
    private ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>(32);
    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private Random random = new Random(System.currentTimeMillis());
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    /**
     * 处理消费者从广播站拉取消息结果
     * @param mq 消息队列
     * @param pullResult 拉取消息结果
     * @param subscriptionData 消息者订阅的主题数据
     * @return
     */
    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,
        final SubscriptionData subscriptionData) {
        //获取拉取结果
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        //更新消费者从这个主题消息队列拉取消息时 建议使用的广播站id
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());
        if (PullStatus.FOUND == pullResult.getPullStatus()) {//如果找到了消息
            //将字节数组 转为byteBuffer
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            //从byteBuffer中解析出消息列表
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            List<MessageExt> msgListFilterAgain = msgList;
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());

                //过滤掉消费者没有订阅的标签
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            //遍历过滤掉订阅标签之后的消息
            for (MessageExt msg : msgListFilterAgain) {
                //是否有事务
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(traFlag)) {
                    //设置消息的事务id
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }

                //向消息的properlies属性中添加消息所在的消费队列的最小偏移量
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                    Long.toString(pullResult.getMinOffset()));
                //向消息的properties属性中添加消息所在的消费队列的最大偏移量
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                    Long.toString(pullResult.getMaxOffset()));
            }

            //将消息添加到找到的消息列表
            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        //设置字节数组为null
        pullResultExt.setMessageBinary(null);

        return pullResult;
    }

    /**
     * 更新某个
     * @param mq
     * @param brokerId
     */
    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    /**
     * 从广播站拉取消息的核心实现
     * @param mq 主题消息队列
     * @param subExpression 消费者订阅的过滤主题消息的表达式
     * @param expressionType 表达式类型
     * @param subVersion 表达式版本
     * @param offset 批量拉取消息的在主题消费队列中的起始偏移量
     * @param maxNums 批量拉取消息的最大值
     * @param sysFlag 系统标志
     * @param commitOffset 消费者组缓存的主题消息队列的消费偏移量
     * @param brokerSuspendMaxTimeMillis 广播站缓存这个请求的最大超时时间
     * @param timeoutMillis 请求超时时间
     * @param communicationMode 与远程广播站的通讯模式 单向、同步、异步
     * @param pullCallback 收到消息后的回调处理
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    public PullResult pullKernelImpl(
        final MessageQueue mq,
        final String subExpression,
        final String expressionType,
        final long subVersion,
        final long offset,
        final int maxNums,
        final int sysFlag,
        final long commitOffset,
        final long brokerSuspendMaxTimeMillis,
        final long timeoutMillis,
        final CommunicationMode communicationMode,
        final PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        //寻找广播站 优先从主题消息队列对应的建议站点获取 否则使用主站
        FindBrokerResult findBrokerResult =
            this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                this.recalculatePullFromWhichNode(mq), false);
        if (null == findBrokerResult) {//没有找到广播站
            //尝试从中心服务器更新广播站列表
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            //再次去寻找广播站
            findBrokerResult =
                this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                    this.recalculatePullFromWhichNode(mq), false);
        }

        if (findBrokerResult != null) {//找到了广播站
            {
                // 主题的表达式类型必须为tag类型
                if (!ExpressionType.isTagType(expressionType)
                    && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", "
                        + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }

            //系统标志位
            int sysFlagInner = sysFlag;

            //远程广播站是从站
            if (findBrokerResult.isSlave()) {//如果站点从站 不能更改广播站主题消息队列的消费偏移量
                //清理掉系统标志位中 可以修改主题消息队列的偏移量的标志位
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            //实例化一个拉取消息的请求头
            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            //设置生产者组名
            requestHeader.setConsumerGroup(this.consumerGroup);
            //设置主题
            requestHeader.setTopic(mq.getTopic());
            //设置消息存储的主题消息队列编号
            requestHeader.setQueueId(mq.getQueueId());
            //设置将要拉取的消息在消息队列中的起始偏移量
            requestHeader.setQueueOffset(offset);
            //设置从主题消息队列中单次批量拉取消息的数量最大值
            requestHeader.setMaxMsgNums(maxNums);
            //设置系统标志
            requestHeader.setSysFlag(sysFlagInner);
            //设置修改广播站主题消息队列的消费偏移量值
            requestHeader.setCommitOffset(commitOffset);
            //设置悬挂消息的超时时间
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            //设置生产者组订阅这个主题过滤消息的表达式
            requestHeader.setSubscription(subExpression);
            //设置生产者订阅这个主题过滤消息的表达式的版本
            requestHeader.setSubVersion(subVersion);
            //设置生产者订阅这个主题过滤消息的表达式类型
            requestHeader.setExpressionType(expressionType);

            //获取广播站地址
            String brokerAddr = findBrokerResult.getBrokerAddr();
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {//如果需要过滤class 过滤当前广播站地址 从可选的广播站地址中选择一个地址
                brokerAddr = computPullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }

            //调用远程客户端 向远程广播站发送一个拉取消息的请求
            PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                brokerAddr,
                requestHeader,
                timeoutMillis,
                communicationMode,
                pullCallback);

            return pullResult;
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    /**
     * 优先从建立的广播站id拉取主题消息队列的消息
     * @param mq 主题消息队列
     * @return
     */
    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        //获取主题消息队列对应的建议站点
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {//存在建议站点
            //获取建议的站点
            return suggest.get();
        }

        //获取主站的id
        return MixAll.MASTER_ID;
    }

    /**
     *
     * @param topic
     * @param brokerAddr
     * @return
     * @throws MQClientException
     */
    private String computPullFromWhichFilterServer(final String topic, final String brokerAddr)
        throws MQClientException {
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);

            //获取过滤当前广播站地址时 可选的广播站地址列表
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {//从可选的广播站地址列表中选择一个地址
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
            + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }
}
