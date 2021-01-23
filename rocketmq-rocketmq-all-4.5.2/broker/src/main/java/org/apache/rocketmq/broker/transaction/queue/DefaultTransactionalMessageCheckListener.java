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

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

/**
 * 默认的事务消息检查监听器类
 */
public class DefaultTransactionalMessageCheckListener extends AbstractTransactionalMessageCheckListener {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    /**
     * 实例化一个默认的事务消息检查监听器
     */
    public DefaultTransactionalMessageCheckListener() {
        super();
    }

    /**
     * 重置将要被丢弃的half message消息
     * 比如消息如果尝试被删除的次数超过了上限值15 或者是消息的生产日期到现在的时间超过了系统日志文件保存的最长时间（72小时）
     * half message将不会被删除half message的消费者消费 被丢弃 重置
     * @param msgExt Message to be discarded.
     */
    @Override
    public void resolveDiscardMsg(MessageExt msgExt) {
        log.error("MsgExt:{} has been checked too many times, so discard it by moving it to system topic TRANS_CHECK_MAXTIME_TOPIC", msgExt);

        try {
            //将消息保存到事务检查最大次数的主题
            MessageExtBrokerInner brokerInner = toMessageExtBrokerInner(msgExt);
            PutMessageResult putMessageResult = this.getBrokerController().getMessageStore().putMessage(brokerInner);
            if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                log.info("Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC OK. Restored in queueOffset={}, " +
                    "commitLogOffset={}, real topic={}", msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
            } else {
                log.error("Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC failed, real topic={}, msgId={}", msgExt.getTopic(), msgExt.getMsgId());
            }
        } catch (Exception e) {
            log.warn("Put checked-too-many-time message to TRANS_CHECK_MAXTIME_TOPIC error. {}", e);
        }

    }

    /**
     * 将生产者生产的消息转为可以存放于广播站的消息
     * @param msgExt 生产者生产的消息
     * @return
     */
    private MessageExtBrokerInner toMessageExtBrokerInner(MessageExt msgExt) {
        //创建一个事务检查最大次数的主题配置
        TopicConfig topicConfig = this.getBrokerController().getTopicConfigManager().createTopicOfTranCheckMaxTime(TCMT_QUEUE_NUMS, PermName.PERM_READ | PermName.PERM_WRITE);

        //获取主题消息队列编号
        int queueId = Math.abs(random.nextInt() % 99999999) % TCMT_QUEUE_NUMS;
        //实例化一个可以保存在广播站的对象
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        //设置主题
        inner.setTopic(topicConfig.getTopicName());
        //设置消息体
        inner.setBody(msgExt.getBody());
        //设置生产者标志
        inner.setFlag(msgExt.getFlag());
        //设置属性
        MessageAccessor.setProperties(inner, msgExt.getProperties());
        //设置属性字符串
        inner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        //设置tagcode值
        inner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgExt.getTags()));
        //设置消息队列编号
        inner.setQueueId(queueId);
        //设置系统标志位
        inner.setSysFlag(msgExt.getSysFlag());
        //设置生产地址
        inner.setBornHost(msgExt.getBornHost());
        //设置生产时间
        inner.setBornTimestamp(msgExt.getBornTimestamp());
        //设置存储地址
        inner.setStoreHost(msgExt.getStoreHost());
        //设置消息被重新消费的次数
        inner.setReconsumeTimes(msgExt.getReconsumeTimes());
        //设置消息id
        inner.setMsgId(msgExt.getMsgId());
        inner.setWaitStoreMsgOK(false);
        return inner;
    }
}
