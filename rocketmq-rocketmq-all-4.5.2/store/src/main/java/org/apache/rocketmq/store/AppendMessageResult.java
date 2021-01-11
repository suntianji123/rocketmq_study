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

/**
 * 向mappedFile存放消息返回状态类
 *
 *
 */
public class AppendMessageResult {

    /**
     * 存储消息的状态
     */
    private AppendMessageStatus status;

    /**
     *  消息在整个commitLog文件夹系统中的起始偏移量
     */
    private long wroteOffset;

    /**
     * 写入到commitLog下的某个mappedFile文件中总的字节数
     */
    private int wroteBytes;

    /**
     * 消息Id 存储消息的host 地址 + 消息在整个commitLog文件夹系统中的起始偏移量
     */
    private String msgId;

    /**
     * 存储消息的时间
     */
    private long storeTimestamp;

    /**
     * 消息在主题队列中index 从1开始
     */
    private long logicsOffset;

    /**
     * 向mappedFile的byteBuffer中写入消息字节数组 消耗的时间
     */
    private long pagecacheRT = 0;

    private int msgNum = 1;

    public AppendMessageResult(AppendMessageStatus status) {
        this(status, 0, 0, "", 0, 0, 0);
    }


    /**
     * 实例化一个向mappedFile中
     * @param status 存储消息的结果状态
     * @param wroteOffset 消息在整个commitLog文件夹系统中的偏移量
     * @param wroteBytes 写入到mappedFile中的字节数
     * @param msgId 消息id storehost+ 消息在整个commitLog文件夹系统中的偏移量
     * @param storeTimestamp 存储消息的时间
     * @param logicsOffset 消息在主题队列中的位置
     * @param pagecacheRT 将消息从写入到文件的byteBuffer消耗的时间
     */
    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, String msgId,
        long storeTimestamp, long logicsOffset, long pagecacheRT) {
        //设置存储的消息结果状态
        this.status = status;
        //设置消息在整个commitLog文件夹系统中的偏移量
        this.wroteOffset = wroteOffset;
        //设置写入到mappedFile文件的字节数
        this.wroteBytes = wroteBytes;
        //设置消息id
        this.msgId = msgId;
        //设置存储消息的时间
        this.storeTimestamp = storeTimestamp;
        //设置消息在主题队列中的index
        this.logicsOffset = logicsOffset;
        //设置将消息写入到问价的bytebuffer消耗时间
        this.pagecacheRT = pagecacheRT;
    }

    public long getPagecacheRT() {
        return pagecacheRT;
    }

    public void setPagecacheRT(final long pagecacheRT) {
        this.pagecacheRT = pagecacheRT;
    }

    public boolean isOk() {
        return this.status == AppendMessageStatus.PUT_OK;
    }

    public AppendMessageStatus getStatus() {
        return status;
    }

    public void setStatus(AppendMessageStatus status) {
        this.status = status;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public long getLogicsOffset() {
        return logicsOffset;
    }

    public void setLogicsOffset(long logicsOffset) {
        this.logicsOffset = logicsOffset;
    }

    public int getMsgNum() {
        return msgNum;
    }

    public void setMsgNum(int msgNum) {
        this.msgNum = msgNum;
    }

    @Override
    public String toString() {
        return "AppendMessageResult{" +
            "status=" + status +
            ", wroteOffset=" + wroteOffset +
            ", wroteBytes=" + wroteBytes +
            ", msgId='" + msgId + '\'' +
            ", storeTimestamp=" + storeTimestamp +
            ", logicsOffset=" + logicsOffset +
            ", pagecacheRT=" + pagecacheRT +
            ", msgNum=" + msgNum +
            '}';
    }
}
