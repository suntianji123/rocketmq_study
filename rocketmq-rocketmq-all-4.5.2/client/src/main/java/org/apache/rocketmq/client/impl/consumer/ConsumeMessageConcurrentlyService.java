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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 实时消息的消费服务类
 */
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();

    /**
     * 消费者实现
     */
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    /**
     * 默认的消息者
     */
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    /**
     * 主题消息监听器
     */
    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;

    /**
     * 消费者消费消息的执行器
     */
    private final ThreadPoolExecutor consumeExecutor;

    /**
     * 消费者组名
     */
    private final String consumerGroup;

    /**
     * 定时执行器
     */
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * 定时清理超时的消息的执行器
     */
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    /**
     * 实时消息的消费实现
     * @param defaultMQPushConsumerImpl 消费者实现
     * @param messageListener 消息监听器
     */
    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerConcurrently messageListener) {
        //设置消费者实现
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        //设置消息监听器
        this.messageListener = messageListener;

        //设置消费者
        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        //设置消费者组名
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        //消费消息的请求任务队列
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        //消费消息的执行器
        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),//最小工作线程数量20
            this.defaultMQPushConsumer.getConsumeThreadMax(),//最大工作线程数量
            1000 * 60,//超过60秒 多余的线程不工作 销毁
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,//消息消息的任务队列
            new ThreadFactoryImpl("ConsumeMessageThread_"));//创建线程的工厂类

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));

        //设置定时清理超时的消息的执行器
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    /**
     * 启动实时消费消息的服务
     * 每隔15分钟 清理掉处理队列中的消费消费失败的消息
     */
    public void start() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                //每隔15分钟 将所有处理队列中消费超时的消息 重新写入到广播站的%RETRY%ConsumerGroupName or %DLQ%ConsumerGroupName主题消息队列
                cleanExpireMsg();
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    /**
     * 关闭消费实时消息的服务
     */
    public void shutdown() {
        //关闭定时器
        this.scheduledExecutorService.shutdown();
        //关闭执行器
        this.consumeExecutor.shutdown();
        //关闭清理超时消息的执行器
        this.cleanExpireMsgExecutors.shutdown();
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // + 1);
        // }
        // log.info("incCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public void decCorePoolSize() {
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize > this.defaultMQPushConsumer.getConsumeThreadMin())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // - 1);
        // }
        // log.info("decCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                RemotingHelper.exceptionSimpleDesc(e),
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }


    /**
     * 消费者提交一个消费请求
     * @param msgs 消息列表
     * @param processQueue 需要被消费的消息所位于的处理中的队列
     * @param messageQueue 消费者分配的主题消息队列
     * @param dispatchToConsume 是否将消息分发给消费者进行消费
     */
    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispatchToConsume) {
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        if (msgs.size() <= consumeBatchSize) {//消息者一次可以消费多个消息
            //实例化一个消费请求
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                //给消费消息的执行器 提交一个消费消息的请求
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }
        } else {
            //将多个消息分批次进行消费
            for (int total = 0; total < msgs.size(); ) {
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                //实例化一个当前批次的消费请求对象
                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    //给消费消息的执行器提价一个消费消息的请求
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {

                    //稍后消费消息
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }


    /**
     * 清理处理中队列超时的消息
     */
    private void cleanExpireMsg() {
        //获取主题消息队列对应的处理中的队列列表
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
            this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {//遍历列表
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            //获取处理中的队列
            ProcessQueue pq = next.getValue();
            //调用处理队列的清理超时消息的方法
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    /**
     * 处理消费结构
     * @param status 消费者消费消息的结果
     * @param context 消息者消费消息的上下文
     * @param consumeRequest 消费者消费消息的请求
     */
    public void processConsumeResult(
        final ConsumeConcurrentlyStatus status,
        final ConsumeConcurrentlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        //获取回复下标
        int ackIndex = context.getAckIndex();

        //消费者没有消费消息
        if (consumeRequest.getMsgs().isEmpty())
            return;

        switch (status) {//消费成功
            case CONSUME_SUCCESS:
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    //设置最后一次消息的下标
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                //消费成功的消息的数量
                int ok = ackIndex + 1;
                //消费失败的消息数量
                int failed = consumeRequest.getMsgs().size() - ok;
                //增加消费者组消费这个主题的消息成功的数量、次数
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                //增加消费者组消费这个主题的消息失败的数量、次数
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;
            case RECONSUME_LATER:
                //稍后再次消费
                ackIndex = -1;
                //增加消费者组消费这个主题的消息失败的数量、次数
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                    consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            case CLUSTERING://消息模型 集群模式
                //消费不成功的需要重新发送给广播站失败的消息列表
                List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {//遍历需要重新消费的消息
                    //获取消息
                    MessageExt msg = consumeRequest.getMsgs().get(i);

                    //将消费不成功的消息重新发送给广播站
                    boolean result = this.sendMessageBack(msg, context);
                    if (!result) {//如果广播站没有接受这个消息 则添加到发送
                        //设置消息被重新消费的次数
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        //推送给广播站失败的消息列表
                        msgBackFailed.add(msg);
                    }
                }

                if (!msgBackFailed.isEmpty()) {//如果有消息推送给广播站失败 只能说明服务器抛出了异常 继续将消息交给消费者消费
                    //移除没有被成功消费的消息
                    consumeRequest.getMsgs().removeAll(msgBackFailed);

                    //重新消息推送给广播站失败的消息
                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        //删除已经消费的消息 返回处理中队列中剩余待消费的消息列表的第一个元素的偏移量
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {//说明处理中的队列还剩余消息没有被消费
            //重新设置主题队列中已经消费到的消息的偏移量
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    /**
     * 消息不能被消费者成功消息 需要重新发送给广播站
     * @param msg 消息
     * @param context 消息被消费的上下文对象
     * @return
     */
    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        //获取控制重新消费次数的来源 -1不重试 直接放入dlq 0 通过广播站控制重新消费次数 1消费者控制重试次数
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // 重新设置消息的主题 包装命名空间
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            //将消息重新推送给广播站
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    /**
     * 提交一个消费请求
     * @param msgs 消息列表
     * @param processQueue 处理中的主题任务队列
     * @param messageQueue 消息队列
     */
    private void submitConsumeRequestLater(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue
    ) {

        //五秒之后 提交一个任务到消费的消息列表
        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * 消费请求
     */
    class ConsumeRequest implements Runnable {

        /**
         * 需要被消费者消费的消息列表
         */
        private final List<MessageExt> msgs;

        /**
         * 消息列表所在的处理中的队列
         */
        private final ProcessQueue processQueue;

        /**
         * 消息列表来源的主题消息队列
         */
        private final MessageQueue messageQueue;

        /**
         * 实例化一个消费请求
         * @param msgs 消息里诶表
         * @param processQueue 处理中的对应
         * @param messageQueue 消息来源的主题消息队列
         */
        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            //设置消息列表
            this.msgs = msgs;
            //设置处理中的queue
            this.processQueue = processQueue;
            //设置主题消息队列
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        /**
         * 处理消费消息的请求
         */
        @Override
        public void run() {
            //如果处理中的队列已经被丢弃  直接return
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            //即时消息的监听器
            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            //实例化一个即时消费上下文
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            //即时消费状态
            ConsumeConcurrentlyStatus status = null;
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            //开始时间
            long beginTimestamp = System.currentTimeMillis();
            //是否有异常
            boolean hasException = false;
            //消费返回的类型
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {//遍历每一个消息
                        //设置每一个消息的消费开始时间
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }

                //消费消息 返回消费的状态
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);

                //有异常 设置有异常标志位true
                hasException = true;
            }

            //消费消息的耗时时间
            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            if (null == status) {
                if (hasException) {
                    //返回类型 有异常
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    //返回类型 null
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {//如果消费消息超过了15分钟
                //设置返回类型为消费超时
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {//状态为消费失败 稍后重试
                //消费消息失败
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {//消费成功
                //设置 返回类型为消费成功
                returnType = ConsumeReturnType.SUCCESS;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                //设置稍后重试
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            //增加消费者消费这个主题的消息的总的耗时
            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            //如果处理中的队列没有被删除
            if (!processQueue.isDropped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
