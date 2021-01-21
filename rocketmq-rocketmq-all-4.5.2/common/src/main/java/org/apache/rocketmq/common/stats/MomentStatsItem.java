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

package org.apache.rocketmq.common.stats;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;

/**
 * 统计项
 */
public class MomentStatsItem {

    /**
     * 统计值
     */
    private final AtomicLong value = new AtomicLong(0);

    /**
     * 统计名
     */
    private final String statsName;

    /**
     * 统计key
     */
    private final String statsKey;

    /**
     * 定时执行器
     */
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * 日志对象
     */
    private final InternalLogger log;

    /**
     * 统计一个统计项
     * @param statsName 统计名
     * @param statsKey 统计key
     * @param scheduledExecutorService 定时执行器
     * @param log 日志
     */
    public MomentStatsItem(String statsName, String statsKey,
        ScheduledExecutorService scheduledExecutorService, InternalLogger log) {
        //设置统计名
        this.statsName = statsName;
        //设置统计key
        this.statsKey = statsKey;
        //设置定时器
        this.scheduledExecutorService = scheduledExecutorService;
        //设置日志对象
        this.log = log;
    }

    public void init() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtMinutes();

                    MomentStatsItem.this.value.set(0);
                } catch (Throwable e) {
                }
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 5, TimeUnit.MILLISECONDS);
    }

    public void printAtMinutes() {
        log.info(String.format("[%s] [%s] Stats Every 5 Minutes, Value: %d",
            this.statsName,
            this.statsKey,
            this.value.get()));
    }

    public AtomicLong getValue() {
        return value;
    }

    public String getStatsKey() {
        return statsKey;
    }

    public String getStatsName() {
        return statsName;
    }
}
