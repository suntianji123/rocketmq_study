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
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 默认的事务服务类
 */
public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    /**
     * 最长删除half message的时间
     */
    private static final int MAX_PROCESS_TIME_LIMIT = 60000;

    /**
     * 尝试删除half message从主题消费队列中没有拉取到half message的最大次数
     */
    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
    }

    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    /**
     * 准备事务消息
     * @param messageInner half message
     * @return
     */
    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        //存放halfMessage
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    /**
     * half message是否需要被丢弃 不交给操作half message的消费者进行消费
     * @param msgExt half message消息对象
     * @param transactionCheckMax 最大被操作的次数 默认为15
     * @return half message的检测次数 超过了上限值
     */
    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        //获取half message当前被操作的次数
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            //获取当前被检测的次数
            checkTime = getInt(checkTimes);
            if (checkTime >= transactionCheckMax) {//检测次数超过了指定的最大次数 需要被丢弃
                return true;
            } else {
                //检测次数加+1
                checkTime++;
            }
        }

        //设置当前被检测的次数 返回不能被丢弃
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    /**
     * 判断是否需要跳过这个half message 不交给操作half message的消费者进行操作
     * @param msgExt half message对象
     * @return
     */
    private boolean needSkip(MessageExt msgExt) {
        //消息的生产日期到现在的时间 超过了系统删除日志文件的时间 者需要被丢弃
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        if (valueOfCurrentMinusBorn
            > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
            * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(
                putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(
                putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug(
                "Send check message, the offset={} restored in queueOffset={} "
                    + "commitLogOffset={} "
                    + "newMsgId={} realMsgId={} topic={}",
                offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                msgExt.getTopic());
            return true;
        } else {
            log.error(
                "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                    + "msgId: {}",
                msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    /**
     * 检查halfmessage
     * @param transactionTimeout 事务超时时间
     * exceed this time interval that can be checked.
     * @param transactionCheckMax 每次检查的half message的最大数量
     * message will be discarded.
     * @param listener 检查事务的监听器
     */
    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
        AbstractTransactionalMessageCheckListener listener) {
        try {
            //half消息主题
            String topic = MixAll.RMQ_SYS_TRANS_HALF_TOPIC;
            //获取half message对应的主题队列列表
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.debug("Check topic={}, queues={}", topic, msgQueues);
            for (MessageQueue messageQueue : msgQueues) {
                //获取开始时间
                long startTime = System.currentTimeMillis();

                //获取half message主题队列对应的操作half message的主题消息队列
                MessageQueue opQueue = getOpQueue(messageQueue);

                //获取half message的消费队列的消费偏移量
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);

                //获取操作half message的消费队列的消费偏移量
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {//消费偏移量不合理
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                        halfOffset, opOffset);
                    continue;
                }

                //已经被操作过的half message在half message主题消费队列中的偏移量列表
                List<Long> doneOpOffset = new ArrayList<>();

                //将要被操作的half message在half message主题消费队列中的偏移量 | 操作half message消息在主题消费队列中的偏移量
                HashMap<Long, Long> removeMap = new HashMap<>();

                //获取已经被操作过的half message偏移量列表 和 将要被操作的half message 与操作它的消息的偏移量的关系表
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                if (null == pullResult) {//为null  m诶找到
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                        messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread
                int getMessageNullCount = 1;
                long newOffset = halfOffset;
                long i = halfOffset;//将要被操作的half message在half message主题消费队列中的偏移量
                while (true) {
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {//如果处理删除half message超时 跳出循环
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    if (removeMap.containsKey(i)) {//这个偏移量的half message将会被删除
                        log.info("Half offset {} has been committed/rolled back", i);
                        //从removeMap中移除
                        removeMap.remove(i);
                    } else {//这个偏移量的half message已经被删除过了
                        GetResult getResult = getHalfMsg(messageQueue, i);

                        //获取half message
                        MessageExt msgExt = getResult.getMsg();
                        if (msgExt == null) {
                            //拉取为null的次数超过了上限
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                    messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                    i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                //设置新的half message在消费队列中的偏移量
                                newOffset = i;
                                continue;
                            }
                        }

                        //拉取到了half message消息 判断half message的检测次数如果超过了上限 则需要被丢弃
                        //half message的生产时间到现在的时间如果超过了72小时 日志文件将会被删除 因为需要跳过这个half message消息
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            //将half message写入到事务检查最大次数的消息队列
                            listener.resolveDiscardMsg(msgExt);
                            //新的消费偏移量+1
                            newOffset = i + 1;
                            i++;
                            continue;
                        }

                        //消息的存储时间超过了开始时间 耗时太久
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.debug("Fresh stored. the miss offset={}, check it later, store={}", i,
                                new Date(msgExt.getStoreTimestamp()));
                            break;
                        }

                        //消息的生产时间到现在的时间
                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        //即时消息的超时时间
                        long checkImmunityTime = transactionTimeout;
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) {
                            //设置当前即时消息的超时时间
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {//时间还没到 不能进行进行事务检测
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt)) {
                                    //新建一个half message存放于主题消息队列
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                    checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }

                        //可以检测事务消息
                        List<MessageExt> opMsg = pullResult.getMsgFoundList();
                        boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                            || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                            || (valueOfCurrentMinusBorn <= -1);

                        if (isNeedCheck) {
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            listener.resolveHalfMsg(msgExt);
                        } else {
                            //获取half message偏移量 | 操作half message的消息偏移量的结果对象
                            pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset);
                            log.info("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i,
                                messageQueue, pullResult);
                            continue;
                        }
                    }

                    newOffset = i + 1;
                    i++;
                }
                if (newOffset != halfOffset) {
                    //修改half message的消费队列消费偏移量
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    //修改操作half message消费队列的消费偏移量
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Check error", e);
        }

    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * 获取操作half message主题消费队列中的消息
     * 找到操作half message的消息 找到对应的half message在half message主题消费队列中的偏移量
     * 将这个偏移量 添加到将要被删除的列表
     * @param removeMap 将要被删除的half message在half message主题消费队列中的偏移量 与操作它的消息 在操作消息队列中的偏移量列表
     * @param opQueue 操作half message的主题消费队列
     * @param pullOffsetOfOp 操作half message的主题消费队列的消费起始偏移量
     * @param miniOffset half message主题消费队列可以被操作的消息的最小偏移量
     * @param doneOpOffset 已经被操作过的half message的偏移量列表
     * @return
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap,
        MessageQueue opQueue, long pullOffsetOfOp, long miniOffset, List<Long> doneOpOffset) {

        //从操作half message的主题消费队列中根据偏移量拉取消息
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, 32);
        if (null == pullResult) {//结果为null 直接返回
            return null;
        }
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
            || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {//偏移量非法或者是没有匹配到消息
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            //更新主题消费队列已经拉取到的消息的偏移量
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {//没有新消息 直接返回
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            return pullResult;
        }

        //获取拉取到的操作消息列表
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {//没有操作消息
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        for (MessageExt opMessageExt : opMsg) {//遍历操作half message的消息猎豹
            //获取将要被操作的half message在half message主题消费队列中的偏移量
            Long queueOffset = getLong(new String(opMessageExt.getBody(), TransactionalMessageUtil.charset));
            log.info("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffset);
            if (TransactionalMessageUtil.REMOVETAG.equals(opMessageExt.getTags())) {
                if (queueOffset < miniOffset) {//如果将要被操作的half message在 half message主题消息队列中的偏移量小于这个主题消息队列的最低偏移量
                    //将操作消息在操作消费队列中的偏移量添加到已经完成的队列列表
                    doneOpOffset.add(opMessageExt.getQueueOffset());
                } else {
                    //需要被删除的half message偏移量 对应的操作消息的偏移量 添加到删除列表
                    removeMap.put(queueOffset, opMessageExt.getQueueOffset());
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }
        }
        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
        MessageExt msgExt) {
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {
            //新建一个half message
            return putImmunityMsgBackToHalfQueue(msgExt);
        } else {
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                if (removeMap.containsKey(prepareQueueOffset)) {
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);
                    return true;
                } else {
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    /**
     * 新建一个half message 放入half message主题消息队列
     * @param messageExt 被克隆的原始消息
     * @return
     */
    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * 从half message主题消息队列中拉取消息
     * @param mq 主题消息队列
     * @param offset 消息位置信息所在的消费队列的消费偏移量
     * @param nums 最大重试次数
     * @return
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * 从操作half message的消费队列中拉取操作消息
     * @param mq 操作half message的消费队列
     * @param offset 操作队列当前的偏移量
     * @param nums 批量拉取消息的最大数量
     * @return
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.valueOf(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.valueOf(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    /**
     * 获取half message队列对应的操作消息队列
     * @param messageQueue half message队列
     * @return
     */
    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        //从缓存中寻找
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {//实例化一个
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                messageQueue.getQueueId());
            //放入列表
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    /**
     * 获取某个消息
     * @param messageQueue 消息所在的消息队列
     * @param offset 消息在消费队列的偏移量
     * @return
     */
    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        //实例化一个获取结果
        GetResult getResult = new GetResult();

        //拉取half message
        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        //设置拉取结果
        getResult.setPullResult(result);
        //获取批量拉取的消息
        List<MessageExt> messageExts = result.getMsgFoundList();
        if (messageExts == null) {
            return getResult;
        }

        //第一条消息Wie指定偏移量的half message
        getResult.setMsg(messageExts.get(0));
        return getResult;
    }

    /**
     * 获取half message
     * @param commitLogOffset halfmessage在commitlog中的偏移量
     * @return
     */
    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        //实例化一个操作结果对象
        OperationResult response = new OperationResult();
        //获取half message
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {//half message不为null
            //设置half message
            response.setPrepareMessage(messageExt);
            //设置响应码
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    /**
     * 删除事务消息的half message
     * @param msgExt half message
     * @return
     */
    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        //实例化一个删除half message的操作消息 位于操作队列 并写入commitlog文件
        if (this.transactionalMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.info("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    /**
     * 提交事务消息
     * @param requestHeader 提交消息的请求头
     * @return
     */
    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    /**
     * 回滚事务
     * @param requestHeader 生产者请求头
     * @return
     */
    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

}
