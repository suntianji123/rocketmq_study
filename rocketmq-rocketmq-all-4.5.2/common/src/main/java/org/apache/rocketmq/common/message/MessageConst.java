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
package org.apache.rocketmq.common.message;

import java.util.HashSet;

public class MessageConst {

    /**
     * Message中的keys在properties中的key
     */
    public static final String PROPERTY_KEYS = "KEYS";

    /**
     * Message的tags值在properties中的key
     */
    public static final String PROPERTY_TAGS = "TAGS";

    /**
     * Messsage的waitStoreMsgOK 在properties中的key
     */
    public static final String PROPERTY_WAIT_STORE_MSG_OK = "WAIT";
    public static final String PROPERTY_DELAY_TIME_LEVEL = "DELAY";

    /**
     * 在消费者组retry or dlq主题消息队列中的消息的原始主题
     */
    public static final String PROPERTY_RETRY_TOPIC = "RETRY_TOPIC";

    /**
     * Message的真正的主题
     */
    public static final String PROPERTY_REAL_TOPIC = "REAL_TOPIC";

    /**
     * Message的真正的主题队列
     */
    public static final String PROPERTY_REAL_QUEUE_ID = "REAL_QID";

    /**
     * 消息属性 是否使用了事务
     */
    public static final String PROPERTY_TRANSACTION_PREPARED = "TRAN_MSG";

    /**
     * 消息属性 生产者组名
     */
    public static final String PROPERTY_PRODUCER_GROUP = "PGROUP";

    /**
     * 消息所在的消费队列的消息的最小偏移量
     */
    public static final String PROPERTY_MIN_OFFSET = "MIN_OFFSET";

    /**
     * 消息所在的消费队列的消息的最大偏移量
     */
    public static final String PROPERTY_MAX_OFFSET = "MAX_OFFSET";
    public static final String PROPERTY_BUYER_ID = "BUYER_ID";

    /**
     * 位于消费者组retry or dlq主题消息队列的消息的原始msgId
     */
    public static final String PROPERTY_ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_ID";
    public static final String PROPERTY_TRANSFER_FLAG = "TRANSFER_FLAG";
    public static final String PROPERTY_CORRECTION_FLAG = "CORRECTION_FLAG";
    public static final String PROPERTY_MQ2_FLAG = "MQ2_FLAG";

    /**
     * 如果消息的主题是重新消费的消息
     */
    public static final String PROPERTY_RECONSUME_TIME = "RECONSUME_TIME";

    /**
     * 广播站消息区域
     */
    public static final String PROPERTY_MSG_REGION = "MSG_REGION";

    /**
     * 广播站是符打开
     */
    public static final String PROPERTY_TRACE_SWITCH = "TRACE_ON";

    /**
     * 设置消息唯一的id 事务ID
     */
    public static final String PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = "UNIQ_KEY";

    /**
     * 消息最大重新消费的次数
     */
    public static final String PROPERTY_MAX_RECONSUME_TIMES = "MAX_RECONSUME_TIMES";

    /**
     * 消息被消费者消费的开始时间
     */
    public static final String PROPERTY_CONSUME_START_TIMESTAMP = "CONSUME_START_TIME";
    public static final String PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET = "TRAN_PREPARED_QUEUE_OFFSET";
    public static final String PROPERTY_TRANSACTION_CHECK_TIMES = "TRANSACTION_CHECK_TIMES";

    /**
     * 即时消息可进行事务检测的最小时间 消息的生产时间到现在的时间与这个值进行比较 比较值超过了这个时间 才能进行事务检测
     */
    public static final String PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS = "CHECK_IMMUNITY_TIME_IN_SECONDS";
    public static final String PROPERTY_INSTANCE_ID = "INSTANCE_ID";

    public static final String KEY_SEPARATOR = " ";

    public static final HashSet<String> STRING_HASH_SET = new HashSet<String>();

    static {
        STRING_HASH_SET.add(PROPERTY_TRACE_SWITCH);
        STRING_HASH_SET.add(PROPERTY_MSG_REGION);
        STRING_HASH_SET.add(PROPERTY_KEYS);
        STRING_HASH_SET.add(PROPERTY_TAGS);
        STRING_HASH_SET.add(PROPERTY_WAIT_STORE_MSG_OK);
        STRING_HASH_SET.add(PROPERTY_DELAY_TIME_LEVEL);
        STRING_HASH_SET.add(PROPERTY_RETRY_TOPIC);
        STRING_HASH_SET.add(PROPERTY_REAL_TOPIC);
        STRING_HASH_SET.add(PROPERTY_REAL_QUEUE_ID);
        STRING_HASH_SET.add(PROPERTY_TRANSACTION_PREPARED);
        STRING_HASH_SET.add(PROPERTY_PRODUCER_GROUP);
        STRING_HASH_SET.add(PROPERTY_MIN_OFFSET);
        STRING_HASH_SET.add(PROPERTY_MAX_OFFSET);
        STRING_HASH_SET.add(PROPERTY_BUYER_ID);
        STRING_HASH_SET.add(PROPERTY_ORIGIN_MESSAGE_ID);
        STRING_HASH_SET.add(PROPERTY_TRANSFER_FLAG);
        STRING_HASH_SET.add(PROPERTY_CORRECTION_FLAG);
        STRING_HASH_SET.add(PROPERTY_MQ2_FLAG);
        STRING_HASH_SET.add(PROPERTY_RECONSUME_TIME);
        STRING_HASH_SET.add(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        STRING_HASH_SET.add(PROPERTY_MAX_RECONSUME_TIMES);
        STRING_HASH_SET.add(PROPERTY_CONSUME_START_TIMESTAMP);
        STRING_HASH_SET.add(PROPERTY_INSTANCE_ID);
    }
}
