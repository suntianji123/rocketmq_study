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
package org.apache.rocketmq.broker.topic;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 主题配置管理器类
 */
public class TopicConfigManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private transient final Lock lockTopicConfigTable = new ReentrantLock();

    /**
     * 主题配置列表 主题对应的主题配置
     */
    private final ConcurrentMap<String, TopicConfig> topicConfigTable =
        new ConcurrentHashMap<String, TopicConfig>(1024);

    /**
     * 数据的版本
     */
    private final DataVersion dataVersion = new DataVersion();

    /**
     * 系统自有的主题列表
     */
    private final Set<String> systemTopicList = new HashSet<String>();

    /**
     * 广播站控制器
     */
    private transient BrokerController brokerController;

    public TopicConfigManager() {
    }

    /**
     * 实例化一个主题配置管理对象
     * @param brokerController 广播站控制器
     */
    public TopicConfigManager(BrokerController brokerController) {
        //设置广播站控制器对象
        this.brokerController = brokerController;
        {
            //测试主题
            String topic = MixAll.SELF_TEST_TOPIC;

            //实例化一个主题配置对象
            TopicConfig topicConfig = new TopicConfig(topic);

            //向系统自有的主题列表添加测试主题
            this.systemTopicList.add(topic);

            //设置主题配置的读队列的数量为1
            topicConfig.setReadQueueNums(1);

            //设置主题配置的写队列的数量为1
            topicConfig.setWriteQueueNums(1);

            //向主题配置列表中放入主题名 主题配置
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // 如果是自动创建主题
            if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                //添加一个默认的主题
                String topic = MixAll.DEFAULT_TOPIC;

                //实例化一个默认的主题配置对象
                TopicConfig topicConfig = new TopicConfig(topic);

                //将默认的主题配置对象添加系统主题列表
                this.systemTopicList.add(topic);

                //设置主题配置对象的读队列数量为8
                topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig()
                    .getDefaultTopicQueueNums());

                //设置主题配置对象的写队列的数量为8
                topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig()
                    .getDefaultTopicQueueNums());

                //默认的主题可继承 可读 可写
                int perm = PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE;

                //设置主题配置的权限值
                topicConfig.setPerm(perm);

                //将主题配置添加到主题配置列表
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
        {
            // MixAll.BENCHMARK_TOPIC
            String topic = MixAll.BENCHMARK_TOPIC;

            //实例化一个基本主题配置
            TopicConfig topicConfig = new TopicConfig(topic);

            //将基本主题配置添加到系统主题配置列表
            this.systemTopicList.add(topic);

            //设置主题的读对垒数量
            topicConfig.setReadQueueNums(1024);

            //设置主题的写队列数量
            topicConfig.setWriteQueueNums(1024);

            //将主题添加到主题配置表
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {

            //获取广播站集群的群名
            String topic = this.brokerController.getBrokerConfig().getBrokerClusterName();

            //实例化一个主题为广播站集群群名的主题配置对象
            TopicConfig topicConfig = new TopicConfig(topic);

            //将主题配置对象添加到系统主题列表
            this.systemTopicList.add(topic);

            //设置权限只能别继承
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getBrokerConfig().isClusterTopicEnable()) {//如果集群主题启动
                //设置权限可以读和写
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }

            //设置主题配置的权限值
            topicConfig.setPerm(perm);

            //将主题配置添加到主推配置列表
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            //获取广播站名
            String topic = this.brokerController.getBrokerConfig().getBrokerName();

            //为广播站实例化一个主题配置对象
            TopicConfig topicConfig = new TopicConfig(topic);

            //向系统主题列表中添加站名主题
            this.systemTopicList.add(topic);
            //设置可继承权限
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getBrokerConfig().isBrokerTopicEnable()) {
                //设置可读 可写权限
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }

            //设置主题配置读队列的数量为1
            topicConfig.setReadQueueNums(1);

            //设置主题配置写队列的数量为1
            topicConfig.setWriteQueueNums(1);

            //设置主题配置的权限值
            topicConfig.setPerm(perm);

            //将主题配置添加到主题配置列表
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            //偏移量移动事件的主题
            String topic = MixAll.OFFSET_MOVED_EVENT;
            //实例化一个主题配置
            TopicConfig topicConfig = new TopicConfig(topic);

            //将主题添加系统主题列表
            this.systemTopicList.add(topic);

            //设置读队列的数量为1
            topicConfig.setReadQueueNums(1);

            //设置写队列的数量为1
            topicConfig.setWriteQueueNums(1);

            //将主题配置添加到主题配置列表
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
    }

    public boolean isSystemTopic(final String topic) {
        return this.systemTopicList.contains(topic);
    }

    public Set<String> getSystemTopic() {
        return this.systemTopicList;
    }

    public boolean isTopicCanSendMessage(final String topic) {
        return !topic.equals(MixAll.DEFAULT_TOPIC);
    }

    public TopicConfig selectTopicConfig(final String topic) {
        return this.topicConfigTable.get(topic);
    }

    public TopicConfig createTopicInSendMessageMethod(final String topic, final String defaultTopic,
        final String remoteAddress, final int clientDefaultTopicQueueNums, final int topicSysFlag) {
        TopicConfig topicConfig = null;
        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null)
                        return topicConfig;

                    TopicConfig defaultTopicConfig = this.topicConfigTable.get(defaultTopic);
                    if (defaultTopicConfig != null) {
                        if (defaultTopic.equals(MixAll.DEFAULT_TOPIC)) {
                            if (!this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                                defaultTopicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
                            }
                        }

                        if (PermName.isInherited(defaultTopicConfig.getPerm())) {
                            topicConfig = new TopicConfig(topic);

                            int queueNums =
                                clientDefaultTopicQueueNums > defaultTopicConfig.getWriteQueueNums() ? defaultTopicConfig
                                    .getWriteQueueNums() : clientDefaultTopicQueueNums;

                            if (queueNums < 0) {
                                queueNums = 0;
                            }

                            topicConfig.setReadQueueNums(queueNums);
                            topicConfig.setWriteQueueNums(queueNums);
                            int perm = defaultTopicConfig.getPerm();
                            perm &= ~PermName.PERM_INHERIT;
                            topicConfig.setPerm(perm);
                            topicConfig.setTopicSysFlag(topicSysFlag);
                            topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                        } else {
                            log.warn("Create new topic failed, because the default topic[{}] has no perm [{}] producer:[{}]",
                                defaultTopic, defaultTopicConfig.getPerm(), remoteAddress);
                        }
                    } else {
                        log.warn("Create new topic failed, because the default topic[{}] not exist. producer:[{}]",
                            defaultTopic, remoteAddress);
                    }

                    if (topicConfig != null) {
                        log.info("Create new topic by default topic:[{}] config:[{}] producer:[{}]",
                            defaultTopic, topicConfig, remoteAddress);

                        this.topicConfigTable.put(topic, topicConfig);

                        this.dataVersion.nextVersion();

                        createNew = true;

                        this.persist();
                    }
                } finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true);
        }

        return topicConfig;
    }

    public TopicConfig createTopicInSendMessageBackMethod(
        final String topic,
        final int clientDefaultTopicQueueNums,
        final int perm,
        final int topicSysFlag) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null)
            return topicConfig;

        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null)
                        return topicConfig;

                    topicConfig = new TopicConfig(topic);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(topicSysFlag);

                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(topic, topicConfig);
                    createNew = true;
                    this.dataVersion.nextVersion();
                    this.persist();
                } finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageBackMethod exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true);
        }

        return topicConfig;
    }

    public void updateTopicUnitFlag(final String topic, final boolean unit) {

        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (unit) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitFlag(oldTopicSysFlag));
            } else {
                topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            this.dataVersion.nextVersion();

            this.persist();
            this.brokerController.registerBrokerAll(false, true);
        }
    }

    public void updateTopicUnitSubFlag(final String topic, final boolean hasUnitSub) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (hasUnitSub) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitSubFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            this.dataVersion.nextVersion();

            this.persist();
            this.brokerController.registerBrokerAll(false, true);
        }
    }

    public void updateTopicConfig(final TopicConfig topicConfig) {
        TopicConfig old = this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (old != null) {
            log.info("update topic config, old:[{}] new:[{}]", old, topicConfig);
        } else {
            log.info("create new topic [{}]", topicConfig);
        }

        this.dataVersion.nextVersion();

        this.persist();
    }

    public void updateOrderTopicConfig(final KVTable orderKVTableFromNs) {

        if (orderKVTableFromNs != null && orderKVTableFromNs.getTable() != null) {
            boolean isChange = false;
            Set<String> orderTopics = orderKVTableFromNs.getTable().keySet();
            for (String topic : orderTopics) {
                TopicConfig topicConfig = this.topicConfigTable.get(topic);
                if (topicConfig != null && !topicConfig.isOrder()) {
                    topicConfig.setOrder(true);
                    isChange = true;
                    log.info("update order topic config, topic={}, order={}", topic, true);
                }
            }

            for (Map.Entry<String, TopicConfig> entry : this.topicConfigTable.entrySet()) {
                String topic = entry.getKey();
                if (!orderTopics.contains(topic)) {
                    TopicConfig topicConfig = entry.getValue();
                    if (topicConfig.isOrder()) {
                        topicConfig.setOrder(false);
                        isChange = true;
                        log.info("update order topic config, topic={}, order={}", topic, false);
                    }
                }
            }

            if (isChange) {
                this.dataVersion.nextVersion();
                this.persist();
            }
        }
    }

    public boolean isOrderTopic(final String topic) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig == null) {
            return false;
        } else {
            return topicConfig.isOrder();
        }
    }

    public void deleteTopicConfig(final String topic) {
        TopicConfig old = this.topicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: {}", old);
            this.dataVersion.nextVersion();
            this.persist();
        } else {
            log.warn("delete topic config failed, topic: {} not exists", topic);
        }
    }

    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    /**
     * 获取配置文件路径
     * @return
     */
    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicConfigPath(this.brokerController.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    /**
     * 解码加载的字符串
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {//字符串不能为null

            //TopicConfig包装的序列化对象 使用json反向序列化 获取对象
            TopicConfigSerializeWrapper topicConfigSerializeWrapper =
                TopicConfigSerializeWrapper.fromJson(jsonString, TopicConfigSerializeWrapper.class);
            if (topicConfigSerializeWrapper != null) {//如果反向序列化的TopicConfig对象不为空
                //将序列化中的主题配置添加到主题配置表
                this.topicConfigTable.putAll(topicConfigSerializeWrapper.getTopicConfigTable());

                //设置数据的版本
                this.dataVersion.assignNewOne(topicConfigSerializeWrapper.getDataVersion());

                //第一次加载 将主题配置打印到日志
                this.printLoadDataWhenFirstBoot(topicConfigSerializeWrapper);
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * 打印主题配置
     * @param tcs TopicConfig被json反应序列化的包装对象
     */
    private void printLoadDataWhenFirstBoot(final TopicConfigSerializeWrapper tcs) {
        //获取包装对象中的主题配置列表
        Iterator<Entry<String, TopicConfig>> it = tcs.getTopicConfigTable().entrySet().iterator();
        while (it.hasNext()) {//遍历每一个实例
            Entry<String, TopicConfig> next = it.next();
            //打印主题配置
            log.info("load exist local topic, {}", next.getValue().toString());
        }
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }
}
