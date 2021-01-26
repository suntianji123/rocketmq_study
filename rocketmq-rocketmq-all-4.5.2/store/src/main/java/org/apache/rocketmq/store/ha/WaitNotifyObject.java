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
package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.HashMap;

/**
 * 等待或者通知的锁对象
 */
public class WaitNotifyObject {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 哪些线程使用当前锁对象 以及线程的阻塞状态
     */
    protected final HashMap<Long/* thread id */, Boolean/* notified */> waitingThreadTable =
        new HashMap<Long, Boolean>(16);

    protected volatile boolean hasNotified = false;

    public void wakeup() {
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }
    }

    /**
     * 阻塞当前线程 等待运行
     * @param interval 阻塞的最大时间
     */
    protected void waitForRunning(long interval) {
        synchronized (this) {
            if (this.hasNotified) {//如果已经通知了其他线程
                this.hasNotified = false;
                this.onWaitEnd();//直接调用onWaitEnd方法
                return;
            }

            try {
                //阻塞一段时间 调用waitOnEnd方法
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.hasNotified = false;
                this.onWaitEnd();
            }
        }
    }

    protected void onWaitEnd() {
    }

    /**
     * 唤醒所有使用当前锁的线程 使其执行任务
     */
    public void wakeupAll() {
        synchronized (this) {
            //是否唤醒过至少一个线程
            boolean needNotify = false;

            for (Boolean value : this.waitingThreadTable.values()) {
                //遍历结果列表
                needNotify = needNotify || !value;
                value = true;
            }

            //如果至少唤醒过一个线程
            if (needNotify) {
                //通知使用当前锁await的线程继续执行
                this.notifyAll();
            }
        }
    }

    /**
     * 所有的线程阻塞interval时间
     * @param interval
     */
    public void allWaitForRunning(long interval) {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            Boolean notified = this.waitingThreadTable.get(currentThreadId);
            if (notified != null && notified) {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
                return;
            }

            try {
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
            }
        }
    }

    /**
     * 移除当前线程的锁状态
     */
    public void removeFromWaitingThreadTable() {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            this.waitingThreadTable.remove(currentThreadId);
        }
    }
}
