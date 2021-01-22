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
 * 从消费队列获取消息的结果枚举
 */
public enum GetMessageStatus {

    /**
     * 找到了消息
     */
    FOUND,

    /**
     * 消息的标签与消费者消息的不签不一致
     */
    NO_MATCHED_MESSAGE,

    /**
     * 从commitlog指定位置获取消息时 返回的字节数为0 说明消息已经从commitlog文件系统中删除
     */
    MESSAGE_WAS_REMOVING,

    /**
     * 没有从commillog中获取到消息
     */
    OFFSET_FOUND_NULL,

    /**
     * 获取消息的偏移量超过了消费队列中消息最大偏移量
     */
    OFFSET_OVERFLOW_BADLY,

    /**
     * 获取消息的偏移量刚好超过了消费队列中消息最大偏移量一个
     */
    OFFSET_OVERFLOW_ONE,

    /**
     * 获取消息的在偏移量小于消费队列的最低偏移量
     */
    OFFSET_TOO_SMALL,

    /**
     * 广播站的消费队列中没有消息
     */
    NO_MATCHED_LOGIC_QUEUE,

    /**
     * 消费队列中没有消息
     */
    NO_MESSAGE_IN_QUEUE,
}
