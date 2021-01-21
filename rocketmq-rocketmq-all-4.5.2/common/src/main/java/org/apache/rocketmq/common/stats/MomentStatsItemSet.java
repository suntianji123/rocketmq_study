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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;

/**
 * 当某个消费者批量从commitlog拉取主题消息队列的消息时  没有拉取到commitlog文件的已写的最大值
 * 记录消费者已经读取的偏移量与commit已经写入的最大值之间的差值
 */
public class MomentStatsItemSet {

    /**
     * 所有掉落的统计项 queueid@topic@consumeGroupName | 统计项
     */
    private final ConcurrentMap<String/* key */, MomentStatsItem> statsItemTable =
        new ConcurrentHashMap<String, MomentStatsItem>(128);

    /**
     * 统计名
     */
    private final String statsName;

    /**
     *延时任务执行器
     */
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * 日志对象
     */
    private final InternalLogger log;

    public MomentStatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, InternalLogger log) {
        //设置统计名
        this.statsName = statsName;
        //设置延时执行器
        this.scheduledExecutorService = scheduledExecutorService;
        //设置日志对象
        this.log = log;
        //初始化统计
        this.init();
    }

    public ConcurrentMap<String, MomentStatsItem> getStatsItemTable() {
        return statsItemTable;
    }

    public String getStatsName() {
        return statsName;
    }

    /**
     * 初始化
     */
    public void init() {
        //每5分钟 打印一次
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 5, TimeUnit.MILLISECONDS);
    }

    /**
     * 每5分钟打印一次
     */
    private void printAtMinutes() {
        //遍历所有掉落的统计项
        Iterator<Entry<String, MomentStatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MomentStatsItem> next = it.next();
            //获取统计项 打印
            next.getValue().printAtMinutes();
        }
    }

    public void setValue(final String statsKey, final int value) {
        MomentStatsItem statsItem = this.getAndCreateStatsItem(statsKey);
        statsItem.getValue().set(value);
    }

    /**
     * 从统计项列表中获取统计项
     * @param statsKey 统计key
     * @return
     */
    public MomentStatsItem getAndCreateStatsItem(final String statsKey) {
        //获取统计项
        MomentStatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null == statsItem) {//统计项为null
            //实例化一个统计项
            statsItem =
                new MomentStatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
            //如果之前已经存在
            MomentStatsItem prev = this.statsItemTable.putIfAbsent(statsKey, statsItem);

            if (null != prev) {
                //将之前的赋值给
                statsItem = prev;
                // statsItem.init();
            }
        }

        //返回统计项
        return statsItem;
    }
}
