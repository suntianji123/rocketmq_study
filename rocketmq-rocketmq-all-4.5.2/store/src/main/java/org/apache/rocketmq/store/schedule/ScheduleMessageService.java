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
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 存在所有延时消息的主题
     * 主题消息队列的数量为延时级别的数量
     */
    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";

    /**
     * 第一次延时的时间
     */
    private static final long FIRST_DELAY_TIME = 1000L;

    /**
     * 延时消费队列中去取出下一个消息的延时时间 然后去判断是否到期
     * 没有到期 继续添加一个延时任务 执行时间为消息的剩余到期时间
     */
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;

    /**
     * 级别对应的延时值 "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h" 级别从1开始
     * 例如级别为3 延时时间为10000毫秒
     */
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<Integer, Long>(32);

    /**
     * 延时主题消息队列 当前级别编号的消费队列的 消费者消费消息的偏移量
     */
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);

    /**
     * 默认的消息存储对象
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * 延时消息服务是否已经启动
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 定时器
     */
    private Timer timer;

    /**
     * 写入消息存储对象
     */
    private MessageStore writeMessageStore;

    /**
     * 最大延时级别
     */
    private int maxDelayLevel;

    /**
     * 实例化一个延时消息服务
     * @param defaultMessageStore 默认的消息存储
     */
    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        //设置默认的消息存储
        this.defaultMessageStore = defaultMessageStore;
        //设置写入的消息存储
        this.writeMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    /**
     * 延时主题有多条延时主题队列 队列数量为延时级别的数量
     * 将延时级别转为延时主题消息队列的编号
     * @param delayLevel 延时级别
     * @return
     */
    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore
     *     the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    /**
     * 根据延时级别 获取延时消息的截止时间 设置到tagscode上
     * @param delayLevel 延时级别
     * @param storeTimestamp 消息的存储时间
     * @return
     */
    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        //获取延时时间
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            //返回未来的时间戳
            return time + storeTimestamp;
        }

        //没有指定级别 默认的延时时间为1秒
        return storeTimestamp + 1000;
    }

    /**
     * 启动延时消息服务
     */
    public void start() {
        if (started.compareAndSet(false, true)) {//之前没有启动 现在启动

            //实例化一个定时器 为守护线程
            this.timer = new Timer("ScheduleMessageTimerThread", true);
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {//遍历延时级别
                /**
                 * 遍历每一个级别对应的延时消费队列 添加一个延时任务 根据offsettable保留的消费偏移量获取消费消息
                 * tagscode值为消息的到期时间  如果消息的到期时间到 将原始消息写入到commitlog文件系统中
                 * 然后 根据下一次消费的偏移量 再次添加一个延时任务
                 */


                //获取延时级别
                Integer level = entry.getKey();
                //获取延时时间
                Long timeDelay = entry.getValue();

                //获取延时主题消息队列（当前级别编号）对应的消费消息偏移量
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    //偏移量为0
                    offset = 0L;
                }

                if (timeDelay != null) {//延时时间不为null 延时1秒之后 执行一次task任务
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
                }
            }

            this.timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (started.get()) ScheduleMessageService.this.persist();
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer)
                this.timer.cancel();
        }

    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public String encode() {
        return this.encode(false);
    }

    /**
     * 加载延时消息服务
     * @return
     */
    public boolean load() {
        //加载配置
        boolean result = super.load();
        //解析级别 对应的延时毫秒值
        result = result && this.parseDelayLevel();
        return result;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * 解析延时级别
     * @return
     */
    public boolean parseDelayLevel() {
        //实例化一个时间单位表
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        //"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            //获取级别列表
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                //延时延时值 + 单位
                String value = levelArray[i];

                //延时时间单位
                String ch = value.substring(value.length() - 1);

                //获取单位对应的毫秒值
                Long tu = timeUnitTable.get(ch);

                //级别
                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }

                //获取时间值
                long num = Long.parseLong(value.substring(0, value.length() - 1));

                //获取延时的毫秒值
                long delayTimeMillis = tu * num;

                //设置级别对应的毫秒值
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    /**
     * 延时消息的定时任务 每一个级别都有一个定时任务
     */
    class DeliverDelayedMessageTimerTask extends TimerTask {
        /**
         * 延时级别
         */
        private final int delayLevel;

        /**
         * 延时级别对应的偏移量
         */
        private final long offset;

        /**
         * 实例化一个延时级别消息的定时任务与
         * @param delayLevel 延时级别（延时消息所在的延时主题消息队列编号）
         * @param offset 当前级别的延时主题消息队列的消费偏移量
         */
        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            //设置延时级别
            this.delayLevel = delayLevel;
            //当前级别对应的延时消费队列的消费偏移量
            this.offset = offset;
        }

        /**
         * 运行方法体
         */
        @Override
        public void run() {
            try {
                if (isStarted()) {//定时器已经启动
                    //定时时间到 执行的方法体
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                //继续添加一个定时任务
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * 正确的到期时间
         * @param now 当前时间
         * @param deliverTimestamp 延时消息的截止时间
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {
            //延时消息的截止时间
            long result = deliverTimestamp;

            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {//这里应该改为maxTimestamp > deliverTimestamp
                result = now;
            }

            return result;
        }

        /**
         * 从当前level对应的延时消费队列的offset位置开始 往后读取所有的消费消息
         * 如果消费消息的定时时间到 将消息重新写入到commitlog文件系统
         */
        public void executeOnTimeup() {
            //根据延时级别获取延时消息所在的延时主题消费队列
            ConsumeQueue cq =
                ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            //失败时候 下一消费的起始偏移量
            long failScheduleOffset = offset;

            if (cq != null) {
                //获取消费偏移量之后的所有的消息位置信息
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {//获取到了结果
                    try {
                        //下一个消费消息的偏移量
                        long nextOffset = offset;
                        int i = 0;
                        //单元模式
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {//遍历字节数 没20个字节为一条消息的偏移量
                            //获取延时消息在commitlog文件系统中的偏移量
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            //获取延时消息在commitlog文件系统的字节数
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            //获取延时消息的生产标签(为延时消息延时的起始时间)
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                        tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            //获取当前时间
                            long now = System.currentTimeMillis();

                            //获取延时时间 tagscode为延时消息的截止时间
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            //下一个消息在消费队列中的偏移量
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            //剩余延时的时间
                            long countdown = deliverTimestamp - now;

                            if (countdown <= 0) {//延时时间到
                                //获取原始消息
                                MessageExt msgExt =
                                    ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                                        offsetPy, sizePy);

                                if (msgExt != null) {//原始消息不为null
                                    try {
                                        //将之前的消息转为广播站可以存储的消息格式
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        //继续将消息写入到commitlog文件系统
                                        PutMessageResult putMessageResult =
                                            ScheduleMessageService.this.writeMessageStore
                                                .putMessage(msgInner);

                                        if (putMessageResult != null
                                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {//存放消息的结果
                                            continue;
                                        } else {
                                            //存放消息失败
                                            // XXX: warn and notify me
                                            log.error(
                                                "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                msgExt.getTopic(), msgExt.getMsgId());
                                            ScheduleMessageService.this.timer.schedule(
                                                new DeliverDelayedMessageTimerTask(this.delayLevel,
                                                    nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel,
                                                nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me
                                         */
                                        log.error(
                                            "ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                                                + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                                                + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else {//当前延时消息的延时时间还未到
                                //再次添加一个延时任务 剩余时间到了之后执行
                                ScheduleMessageService.this.timer.schedule(
                                    new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                    countdown);
                                //更新当前消费队列的消费偏移量
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        //延时消费队列中下一个消息的偏移量
                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                        //再次添加一个延时任务 100毫秒之后 从起始偏移量开始 再次获取
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                            this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        //更新主题消费队列的消费偏移量
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {
                        //释放
                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {
                    //获取消费队列最小偏移量
                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        //失败时候下一个的消费的起始偏移量
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                            + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            //再次添加一个延时任务 从起始的偏移量开始 将延时时间到了的消息写入commitlog文件系统
            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        /**
         * 将消息转为存储在broker中的消息
         * @param msgExt 消息体
         * @return
         */
        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            //实例化一个可以存储在广播站中的消息实例
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            //设置消息体
            msgInner.setBody(msgExt.getBody());
            //设置生产者标志位
            msgInner.setFlag(msgExt.getFlag());
            //设置属性
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            //获取主题过滤类型
            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            //获取生产标签纸
            long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            //设置生产标签纸
            msgInner.setTagsCode(tagsCodeValue);
            //设置消息properties map字符串格式
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            //设置系统标志位
            msgInner.setSysFlag(msgExt.getSysFlag());
            //设置消息的生产时间
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            //设置消息的生产地址
            msgInner.setBornHost(msgExt.getBornHost());
            //设置消息的存储地址
            msgInner.setStoreHost(msgExt.getStoreHost());
            //设置消息重新被消费的次数
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);

            //清除延时级别
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            //设置消息的主题为真正的主题
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            //设置消息真正的主题消息队列编号
            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
