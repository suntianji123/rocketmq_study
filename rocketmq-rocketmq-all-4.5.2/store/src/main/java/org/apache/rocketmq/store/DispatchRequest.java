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
package org.apache.rocketmq.store;

import java.util.Map;

/**
 * 分发请求
 */
public class DispatchRequest {

    /**
     * 主题
     */
    private final String topic;

    /**
     * 消息所在的主题消息队列编号
     */
    private final int queueId;

    /**
     * 消息在主题队列中的偏移量
     */
    private final long commitLogOffset;

    /**
     * 消息大小
     */
    private int msgSize;

    /**
     * 标签的哈希值
     */
    private final long tagsCode;

    /**
     * 存储时间
     */
    private final long storeTimestamp;

    /**
     * 消息在主题消息队列中的偏移量
     */
    private final long consumeQueueOffset;

    /**
     * 关键字
     */
    private final String keys;
    private final boolean success;
    private final String uniqKey;

    /**
     * 系统标志位
     */
    private final int sysFlag;

    /**
     * half message在commitlog中位置
     */
    private final long preparedTransactionOffset;

    /**
     * 属性列表
     */
    private final Map<String, String> propertiesMap;
    private byte[] bitMap;

    private int bufferSize = -1;//the buffer size maybe larger than the msg size if the message is wrapped by something

    /**
     * 实例化一个分发请求
     * @param topic 主题
     * @param queueId 消息所在的主题消息队列
     * @param commitLogOffset 消息在commitlog中的偏移量
     * @param msgSize 消息大小
     * @param tagsCode tag的哈希值
     * @param storeTimestamp 消息生产时间
     * @param consumeQueueOffset 消息在主题消息队列中的偏移量
     * @param keys 关键字
     * @param uniqKey 事务id
     * @param sysFlag 系统标志位
     * @param preparedTransactionOffset half message在commitlog中的偏移量
     * @param propertiesMap 属性值map
     */
    public DispatchRequest(
        final String topic,
        final int queueId,
        final long commitLogOffset,
        final int msgSize,
        final long tagsCode,
        final long storeTimestamp,
        final long consumeQueueOffset,
        final String keys,
        final String uniqKey,
        final int sysFlag,
        final long preparedTransactionOffset,
        final Map<String, String> propertiesMap
    ) {
        //设置主题
        this.topic = topic;
        //设置消息队列编号
        this.queueId = queueId;
        //设置消息在commitlog中的偏移量
        this.commitLogOffset = commitLogOffset;
        //谁知消息大小
        this.msgSize = msgSize;
        //设置tag的哈希值
        this.tagsCode = tagsCode;
        //设置消息生产的时间
        this.storeTimestamp = storeTimestamp;
        //设置消息在主题队列中的偏移量
        this.consumeQueueOffset = consumeQueueOffset;
        //设置关键字
        this.keys = keys;
        //设置事务id
        this.uniqKey = uniqKey;
        //设置系统标志位
        this.sysFlag = sysFlag;
        //设置half message在commitlog中的偏移量
        this.preparedTransactionOffset = preparedTransactionOffset;
        this.success = true;
        //设置属性值map
        this.propertiesMap = propertiesMap;
    }

    public DispatchRequest(int size) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = false;
        this.propertiesMap = null;
    }

    public DispatchRequest(int size, boolean success) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = success;
        this.propertiesMap = null;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public long getConsumeQueueOffset() {
        return consumeQueueOffset;
    }

    public String getKeys() {
        return keys;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getUniqKey() {
        return uniqKey;
    }

    public Map<String, String> getPropertiesMap() {
        return propertiesMap;
    }

    public byte[] getBitMap() {
        return bitMap;
    }

    public void setBitMap(byte[] bitMap) {
        this.bitMap = bitMap;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
