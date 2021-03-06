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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InnerLoggerFactory;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事务消息桥梁类
 */
public class TransactionalMessageBridge {
    private static final InternalLogger LOGGER = InnerLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    /**
     * half message主题消息队列 | 操作half message主题消息队列
     */
    private final ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    /**
     *广播站控制器
     */
    private final BrokerController brokerController;

    /**
     * 广播站消息存储
     */
    private final MessageStore store;

    /**
     * 事务消息存储端口
     */
    private final SocketAddress storeHost;

    /**
     * 实例化一个事务消息桥梁对象
     * @param brokerController 广播站控制器
     * @param store 广播站消息存储对象
     */
    public TransactionalMessageBridge(BrokerController brokerController, MessageStore store) {
        try {
            //设置广播站控制器
            this.brokerController = brokerController;
            //设置广播站消息存储对象
            this.store = store;

            //设置时间消息存储端口
            this.storeHost =
                new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(),
                    brokerController.getNettyServerConfig().getListenPort());
        } catch (Exception e) {
            LOGGER.error("Init TransactionBridge error", e);
            throw new RuntimeException(e);
        }

    }

    /**
     * 返回消费half message的主题队列的消费者队列对应的偏移量
     * @param mq half message主题队列
     * @return
     */
    public long fetchConsumeOffset(MessageQueue mq) {
        //获取half message消费队列的消费偏移量
        long offset = brokerController.getConsumerOffsetManager().queryOffset(TransactionalMessageUtil.buildConsumerGroup(),
            mq.getTopic(), mq.getQueueId());
        if (offset == -1) {
            //最低消费偏移量
            offset = store.getMinOffsetInQueue(mq.getTopic(), mq.getQueueId());
        }
        return offset;
    }

    /**
     * 寻找某个主推对应的主题队列列表
     * @param topic
     * @return
     */
    public Set<MessageQueue> fetchMessageQueues(String topic) {
        //主题队列集合
        Set<MessageQueue> mqSet = new HashSet<>();
        //获取half message主题配置
        TopicConfig topicConfig = selectTopicConfig(topic);
        if (topicConfig != null && topicConfig.getReadQueueNums() > 0) {
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                //实例化主题队列
                MessageQueue mq = new MessageQueue();
                //设置主题
                mq.setTopic(topic);
                //设置广播站名
                mq.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
                //设置队列编号
                mq.setQueueId(i);
                //添加主题队列
                mqSet.add(mq);
            }
        }
        return mqSet;
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.brokerController.getConsumerOffsetManager().commitOffset(
            RemotingHelper.parseSocketAddressAddr(this.storeHost), TransactionalMessageUtil.buildConsumerGroup(), mq.getTopic(),
            mq.getQueueId(), offset);
    }

    /**
     * 从half message主题消息队列中拉取某条消息
     * @param queueId 消费队列编号
     * @param offset  消费队列偏移量
     * @param nums 最大重试次数
     * @return
     */
    public PullResult getHalfMessage(int queueId, long offset, int nums) {
        //构建操作half message的消费者组名
        String group = TransactionalMessageUtil.buildConsumerGroup();
        //构建消费half message的主题
        String topic = TransactionalMessageUtil.buildHalfTopic();
        //实例化一个订阅数据
        SubscriptionData sub = new SubscriptionData(topic, "*");
        //获取结果
        return getMessage(group, topic, queueId, offset, nums, sub);
    }

    /**
     * 拉取操作消息
     * @param queueId 操作half message主题消息队列的编号
     * @param offset 当前操作half message主题消息队列的消费偏移量
     * @param nums 批量拉取消息的最大数量
     * @return
     */
    public PullResult getOpMessage(int queueId, long offset, int nums) {
        //构建操作half message主题消息队列的消费者组名
        String group = TransactionalMessageUtil.buildConsumerGroup();
        //构建操作half message的主题
        String topic = TransactionalMessageUtil.buildOpTopic();
        //实例haul一个订阅
        SubscriptionData sub = new SubscriptionData(topic, "*");
        return getMessage(group, topic, queueId, offset, nums, sub);
    }

    /**
     * 从主题的某个消息队列中拉取消息
     * @param group 订阅主题的消费者组名
     * @param topic 主题
     * @param queueId 主题的消息队列编号
     * @param offset 消费队列的消费偏移量
     * @param nums 批量拉取消息的最大值
     * @param sub 消费者订阅配置
     * @return
     */
    private PullResult getMessage(String group, String topic, int queueId, long offset, int nums,
        SubscriptionData sub) {
        //从主题消费队列的指定偏移量开始拉取消息
        GetMessageResult getMessageResult = store.getMessage(group, topic, queueId, offset, nums, null);

        if (getMessageResult != null) {//拉取消息结果不为null
            //初始化拉取消息的结果为没有新的消息
            PullStatus pullStatus = PullStatus.NO_NEW_MSG;
            List<MessageExt> foundList = null;
            switch (getMessageResult.getStatus()) {
                case FOUND://如果拉取到了消息
                    pullStatus = PullStatus.FOUND;
                    //解码 获取消息列表
                    foundList = decodeMsgList(getMessageResult);

                    //增加统计消费者组从主题消费队列拉取消息的总数
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(group, topic,
                        getMessageResult.getMessageCount());

                    //统计消息者组从主题消费队列拉取消息的总的字节数
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(group, topic,
                        getMessageResult.getBufferTotalSize());

                    //增加统计广播站给消费者提供消息的总数
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(getMessageResult.getMessageCount());

                    //统计主题消费队列拉取消息的时间
                    this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
                        this.brokerController.getMessageStore().now() - foundList.get(foundList.size() - 1)
                            .getStoreTimestamp());
                    break;
                case NO_MATCHED_MESSAGE:
                    //没有找到消息
                    pullStatus = PullStatus.NO_MATCHED_MSG;
                    LOGGER.warn("No matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case NO_MESSAGE_IN_QUEUE:
                    //主题消费队列中没有消息
                    pullStatus = PullStatus.NO_NEW_MSG;
                    LOGGER.warn("No new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case MESSAGE_WAS_REMOVING:
                case NO_MATCHED_LOGIC_QUEUE:
                case OFFSET_FOUND_NULL:
                case OFFSET_OVERFLOW_BADLY:
                case OFFSET_OVERFLOW_ONE:
                case OFFSET_TOO_SMALL:
                    //偏移量非法
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    LOGGER.warn("Offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                default:
                    assert false;
                    break;
            }

            //返回拉取消息的结果
            return new PullResult(pullStatus, getMessageResult.getNextBeginOffset(), getMessageResult.getMinOffset(),
                getMessageResult.getMaxOffset(), foundList);

        } else {
            LOGGER.error("Get message from store return null. topic={}, groupId={}, requestOffset={}", topic, group,
                offset);
            return null;
        }
    }

    private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                MessageExt msgExt = MessageDecoder.decode(bb);
                foundList.add(msgExt);
            }

        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    /**
     * 存放halfMessage
     * @param messageInner 消息
     * @return
     */
    public PutMessageResult putHalfMessage(MessageExtBrokerInner messageInner) {
        return store.putMessage(parseHalfMessageInner(messageInner));
    }

    /**
     * 解析halfMessage
     * @param msgInner 消息
     * @return
     */
    private MessageExtBrokerInner parseHalfMessageInner(MessageExtBrokerInner msgInner) {
        //向msgInner的properties的保留真正的主题
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC, msgInner.getTopic());
        //向msgInner的properties保留原来的主题编号
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID,
            String.valueOf(msgInner.getQueueId()));

        //设置commit rollback位的值为0
        msgInner.setSysFlag(
            MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), MessageSysFlag.TRANSACTION_NOT_TYPE));
        //设置half message主题
        msgInner.setTopic(TransactionalMessageUtil.buildHalfTopic());
        //设置half message的队列编号
        msgInner.setQueueId(0);
        //设置properties字符串
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        return msgInner;
    }

    /**
     * 操作事务消息
     * @param messageExt 事务消息
     * @param opType 操作类型
     * @return
     */
    public boolean putOpMessage(MessageExt messageExt, String opType) {
        //实例化一个存储half message的主题消息队列对象
        MessageQueue messageQueue = new MessageQueue(messageExt.getTopic(),
            this.brokerController.getBrokerConfig().getBrokerName(), messageExt.getQueueId());
        if (TransactionalMessageUtil.REMOVETAG.equals(opType)) {//如果是删除
            //添加一个操作half message的消息到操作half message主题消息的主题消息队列
            return addRemoveTagInTransactionOp(messageExt, messageQueue);
        }
        return true;
    }

    public PutMessageResult putMessageReturnResult(MessageExtBrokerInner messageInner) {
        LOGGER.debug("[BUG-TO-FIX] Thread:{} msgID:{}", Thread.currentThread().getName(), messageInner.getMsgId());
        return store.putMessage(messageInner);
    }

    public boolean putMessage(MessageExtBrokerInner messageInner) {
        PutMessageResult putMessageResult = store.putMessage(messageInner);
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            return true;
        } else {
            LOGGER.error("Put message failed, topic: {}, queueId: {}, msgId: {}",
                messageInner.getTopic(), messageInner.getQueueId(), messageInner.getMsgId());
            return false;
        }
    }

    /**
     * 再创建一个新的half message
     * @param msgExt 原始half message
     * @return
     */
    public MessageExtBrokerInner renewImmunityHalfMessageInner(MessageExt msgExt) {
        //克隆一个新的half message
        MessageExtBrokerInner msgInner = renewHalfMessageInner(msgExt);
        //获取原始half message的前一个偏移量
        String queueOffsetFromPrepare = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null != queueOffsetFromPrepare) {//存在前一个偏移量
            //设置新克隆的half message的前一个偏移量为老的偏移量
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
                String.valueOf(queueOffsetFromPrepare));
        } else {
            //设置新克隆的half message的前一个half message的偏移量为原始消息的偏移量
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
                String.valueOf(msgExt.getQueueOffset()));
        }

        //设置属性
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        //返回克隆后的消息
        return msgInner;
    }

    public MessageExtBrokerInner renewHalfMessageInner(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getTopic());
        msgInner.setBody(msgExt.getBody());
        msgInner.setQueueId(msgExt.getQueueId());
        msgInner.setMsgId(msgExt.getMsgId());
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setTags(msgExt.getTags());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setWaitStoreMsgOK(false);
        return msgInner;
    }

    /**
     * 创建一个操作消息对应的写入到磁盘的消息
     * @param message 操作消息
     * @param messageQueue 操作消息将要存放的队列
     * @return
     */
    private MessageExtBrokerInner makeOpMessageInner(Message message, MessageQueue messageQueue) {
        //实例化一个消息
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        //设置操作消息主题
        msgInner.setTopic(message.getTopic());
        //设置操作消息的消息体
        msgInner.setBody(message.getBody());
        //设置队列编号
        msgInner.setQueueId(messageQueue.getQueueId());
        //设置标签
        msgInner.setTags(message.getTags());
        //设置标签的哈希值
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
        //设置系统标志
        msgInner.setSysFlag(0);
        //设置properties map
        MessageAccessor.setProperties(msgInner, message.getProperties());
        //设置properties map->String
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(message.getProperties()));
        //设置生产消息的时间
        msgInner.setBornTimestamp(System.currentTimeMillis());
        //设置生产消息的地址
        msgInner.setBornHost(this.storeHost);
        //设置存储消息的地址
        msgInner.setStoreHost(this.storeHost);
        msgInner.setWaitStoreMsgOK(false);
        //设置uninid
        MessageClientIDSetter.setUniqID(msgInner);
        return msgInner;
    }

    private TopicConfig selectTopicConfig(String topic) {
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (topicConfig == null) {
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                topic, 1, PermName.PERM_WRITE | PermName.PERM_READ, 0);
        }
        return topicConfig;
    }

    /**
     * 添加一个删除的消息在half message的主题消息队列对应的操作主题消息队列
     * @param messageExt 将会被删除的half消息
     * @param messageQueue 主题队列
     * @return
     */
    private boolean addRemoveTagInTransactionOp(MessageExt messageExt, MessageQueue messageQueue) {
        //实例化一个消息 主题：操作half message tag : d(删除) 消息体：在主题队列中的偏移量
        Message message = new Message(TransactionalMessageUtil.buildOpTopic(), TransactionalMessageUtil.REMOVETAG,
            String.valueOf(messageExt.getQueueOffset()).getBytes(TransactionalMessageUtil.charset));
        //将操作消息 写入到主题队列
        writeOp(message, messageQueue);
        return true;
    }

    /**
     * 将操作消息添加操作消息对应的操作队列
     * @param message 操作消息
     * @param mq 将要被操作的消息位于的主题队列
     */
    private void writeOp(Message message, MessageQueue mq) {
        //获取操作队列
        MessageQueue opQueue;
        if (opQueueMap.containsKey(mq)) {//主题队列之前创建过与之对应的操作队列
            //获取操作队列
            opQueue = opQueueMap.get(mq);
        } else {//之前没有创建过half message对应的操作队列
            //根据half message主题消息队列创建一个操作的half message的消息队列
            opQueue = getOpQueueByHalf(mq);
            //将创建队列放入opqueueMap
            MessageQueue oldQueue = opQueueMap.putIfAbsent(mq, opQueue);
            if (oldQueue != null) {
                opQueue = oldQueue;
            }
        }
        if (opQueue == null) {//操作队列不存在 继续创建
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), mq.getBrokerName(), mq.getQueueId());
        }

        //将操作消息写入commitlog文件
        putMessage(makeOpMessageInner(message, opQueue));
    }

    /**
     * 通过half message的主题构建一个操作half message的主题
     * @param halfMQ half message所在的队列的主题
     * @return
     */
    private MessageQueue getOpQueueByHalf(MessageQueue halfMQ) {
        //实例化一个操作队列
        MessageQueue opQueue = new MessageQueue();
        //设置操作队列的主题
        opQueue.setTopic(TransactionalMessageUtil.buildOpTopic());
        //设置操作队列的广播站
        opQueue.setBrokerName(halfMQ.getBrokerName());
        //设置操作队列的编号
        opQueue.setQueueId(halfMQ.getQueueId());
        return opQueue;
    }

    /**
     * 根据消息在commitLog中的偏移量查询half message
     * @param commitLogOffset 偏移量
     * @return
     */
    public MessageExt lookMessageByOffset(final long commitLogOffset) {
        return this.store.lookMessageByOffset(commitLogOffset);
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }
}
