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

import java.nio.ByteBuffer;
import org.apache.rocketmq.common.message.MessageExtBatch;

/**
 * 向mappedFile文件写入msg的接口
 */
public interface AppendMessageCallback {

    /**
     * 向某个mappedFile写入消息
     * @param fileFromOffset mappedFile在整个文件夹系统的起始偏移量
     * @param byteBuffer 文件的ByteBuffer额对象
     * @param maxBlank 最大可写的孔家
     * @param msg 消息对象
     * @return
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
        final int maxBlank, final MessageExtBrokerInner msg);

    /**
     * After batched message serialization, write MapedByteBuffer
     *
     * @param messageExtBatch, backed up by a byte array
     * @return How many bytes to write
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
        final int maxBlank, final MessageExtBatch messageExtBatch);
}
