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

/**
 * $Id: SendMessageRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

/**
 * 向广播站推送消息的请求头类
 */
public class SendMessageRequestHeader implements CommandCustomHeader {

    /**
     * 消息的来源生产者组名
     */
    @CFNotNull
    private String producerGroup;

    /**
     * 消息所属的主题
     */
    @CFNotNull
    private String topic;


    /**
     * 默认主题
     */
    @CFNotNull
    private String defaultTopic;

    /**
     * 默认主题对应的队列数量
     */
    @CFNotNull
    private Integer defaultTopicQueueNums;

    /**
     * 队列编号
     */
    @CFNotNull
    private Integer queueId;

    /**
     * 消息的系统标志位  比如消息的消息体是否经过了zip压缩处理 消息是否有产权
     */
    @CFNotNull
    private Integer sysFlag;

    //请求的开始时间
    @CFNotNull
    private Long bornTimestamp;

    //消息的自定义标志位
    @CFNotNull
    private Integer flag;

    /**
     * 消息的properties map转为的字符串格式
     */
    @CFNullable
    private String properties;

    /**
     * 消息当前被重新消费的次数
     */
    @CFNullable
    private Integer reconsumeTimes;

    /**
     * 消息的生产者是否为单元模式
     */
    @CFNullable
    private boolean unitMode = false;

    /**
     * 是否为批量消息
     */
    @CFNullable
    private boolean batch = false;

    /**
     * 消息最大重新消费的次数
     */
    private Integer maxReconsumeTimes;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public Integer getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(Integer defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Integer getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(Integer sysFlag) {
        this.sysFlag = sysFlag;
    }

    public Long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(Long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public Integer getFlag() {
        return flag;
    }

    public void setFlag(Integer flag) {
        this.flag = flag;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public Integer getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(Integer reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public Integer getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final Integer maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public boolean isBatch() {
        return batch;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }
}
