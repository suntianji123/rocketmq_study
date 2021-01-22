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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;

/**
 * 处理中的队列
 */
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME =
        Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 读写锁
     */
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();

    /**
     * 广播站消费队列中 偏移量对应的消息 偏移量 | 消息
     */
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();

    /**
     * 从广播站消费队列拉取到的消息的总的数量
     */
    private final AtomicLong msgCount = new AtomicLong();

    /**
     * 从广播站的消费队列获取到的合法的消息的body总的大小
     */
    private final AtomicLong msgSize = new AtomicLong();
    private final Lock lockConsume = new ReentrantLock();
    /**
     * A subset of msgTreeMap, will only be used when orderly consume
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);

    /**
     * 从广播站的消费队列获取到的消息的最大偏移量
     */
    private volatile long queueOffsetMax = 0L;

    /**
     * 是否被丢弃
     */
    private volatile boolean dropped = false;

    /**
     * 上一次拉取的时间
     */
    private volatile long lastPullTimestamp = System.currentTimeMillis();

    /**
     * 最后一次消费处理中队列中的消息的时间
     */
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();
    private volatile boolean locked = false;
    private volatile long lastLockTimestamp = System.currentTimeMillis();

    /**
     * 处理中的队列的消息是否正在将从广播站消费队列拉取到的消息下发给消费者进行处理
     */
    private volatile boolean consuming = false;

    /**
     * 设置广播站消费队列消息的最大偏移量
     */
    private volatile long msgAccCnt = 0;

    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * @param pushConsumer
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }

        int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16;
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                this.lockTreeMap.readLock().lockInterruptibly();
                try {
                    if (!msgTreeMap.isEmpty() && System.currentTimeMillis() - Long.parseLong(MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue())) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                        msg = msgTreeMap.firstEntry().getValue();
                    } else {

                        break;
                    }
                } finally {
                    this.lockTreeMap.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {

                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    this.lockTreeMap.writeLock().lockInterruptibly();
                    try {
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        this.lockTreeMap.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    /**
     * 向处理中的队列中添加从广播站拉取的消息列表
     * @param msgs 从广播站拉取的消息列表
     * @return 是否将消息下发给消费者的消息监听器进行处理
     */
    public boolean putMessage(final List<MessageExt> msgs) {
        boolean dispatchToConsume = false;
        try {
            //打开写锁
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                //合法的消息数量
                int validMsgCnt = 0;
                for (MessageExt msg : msgs) {//遍历消息
                    //将偏移量对应的消息缓存到msgTreemap
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    if (null == old) {//之前这个偏移量出的消息不存在
                        //增加合法的消息数量
                        validMsgCnt++;
                        //设置最大偏移量值
                        this.queueOffsetMax = msg.getQueueOffset();

                        //增加消息大小
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }

                //增加从广播站拉取到的消息总的数量
                msgCount.addAndGet(validMsgCnt);

                if (!msgTreeMap.isEmpty() && !this.consuming) { //有新消息达到 并且没有将消息下发给消费者进行消息
                    //需要将消息下发给消费者进行消费
                    dispatchToConsume = true;
                    //设置这个处理中的队列的消息正在被消费者消息
                    this.consuming = true;
                }

                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1);

                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) {
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            //广播站消费队列消息的最大偏移量
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                //释放写锁
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }

    /**
     * 处理队列中的正等待被消费的消息 最后一个消息在主题消息队列中的偏移量和第一个消息在主题消息队列中的偏移量的差值
     * @return
     */
    public long getMaxSpan() {
        try {
            //打开读锁
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                //等待被消费的消息列表不为空
                if (!this.msgTreeMap.isEmpty()) {
                    //返回最后一个消息的偏移量 - 第一个消息的偏移量
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                //释放读锁
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    /**
     * 从处理中的队列中删除一些消息 返回处理中队列待消费的消息列表的第一个元素的偏移量
     * @param msgs 将要被删除的消息列表
     * @return
     */
    public long removeMessage(final List<MessageExt> msgs) {
        //处理队列中剩余的消息的数量
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            //打开写锁
            this.lockTreeMap.writeLock().lockInterruptibly();

            //设置最后一次消费队列中的消息的时间
            this.lastConsumeTimestamp = now;
            try {
                if (!msgTreeMap.isEmpty()) {
                    //结果偏移量
                    result = this.queueOffsetMax + 1;
                    //移除的消息的数量
                    int removedCnt = 0;
                    for (MessageExt msg : msgs) {
                        //删除这个偏移量的消息
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        if (prev != null) {
                            //移除的数量-1
                            removedCnt--;
                            //减少处理中队列的总的字节数
                            msgSize.addAndGet(0 - msg.getBody().length);
                        }
                    }

                    //减少处理中队列的消息的总数
                    msgCount.addAndGet(removedCnt);

                    if (!msgTreeMap.isEmpty()) {
                        //msg列表剩余的消息列表的第一个元素的偏移量
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                //释放写锁
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public void rollback() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long commit() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                    msgSize.addAndGet(0 - msg.getBody().length);
                }
                this.consumingMsgOrderlyTreeMap.clear();
                if (offset != null) {
                    return offset + 1;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }

    public void makeMessageToCosumeAgain(List<MessageExt> msgs) {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    public List<MessageExt> takeMessags(final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            result.add(entry.getValue());
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            break;
                        }
                    }
                }

                if (result.isEmpty()) {
                    consuming = false;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    public boolean hasTempMessage() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
        }

        return true;
    }

    public void clear() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public Lock getLockConsume() {
        return lockConsume;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.lockTreeMap.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }
}
