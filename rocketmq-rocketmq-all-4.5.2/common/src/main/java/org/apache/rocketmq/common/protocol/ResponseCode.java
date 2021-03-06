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

package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

public class ResponseCode extends RemotingSysResponseCode {

    public static final int FLUSH_DISK_TIMEOUT = 10;

    public static final int SLAVE_NOT_AVAILABLE = 11;

    public static final int FLUSH_SLAVE_TIMEOUT = 12;

    public static final int MESSAGE_ILLEGAL = 13;

    public static final int SERVICE_NOT_AVAILABLE = 14;

    public static final int VERSION_NOT_SUPPORTED = 15;

    /**
     * 响应码 没有权限（比如广播站拒绝事务消息）
     */
    public static final int NO_PERMISSION = 16;

    /**
     * 主题不存在
     */
    public static final int TOPIC_NOT_EXIST = 17;
    public static final int TOPIC_EXIST_ALREADY = 18;

    /**
     * 消费者从广播站拉取消息  广播站响应状态码:没有找到消息
     */
    public static final int PULL_NOT_FOUND = 19;

    /**
     * 消费者从广播站拉取消息 广播站响应状态码 立刻尝试重新发起请求
     */
    public static final int PULL_RETRY_IMMEDIATELY = 20;

    public static final int PULL_OFFSET_MOVED = 21;

    public static final int QUERY_NOT_FOUND = 22;

    /**
     * 消费者组从广播站主题消息队列拉取消息时 传来了过滤主题的表达式以及过滤类型 但广播站解析成消费者的订阅数据出错
     */
    public static final int SUBSCRIPTION_PARSE_FAILED = 23;

    /**
     * 消费者组从广播站主题消息队列拉取消息时 广播站根据消费者组名 获取不到消息者组信息 返回这个错误
     */
    public static final int SUBSCRIPTION_NOT_EXIST = 24;

    /**
     * 消费者组从广播站主题消息队列拉取消息时 如果广播站缓存的这个主题的订阅配置的版本小于消费者传过来的主题订阅版本 返回这个错误
     */
    public static final int SUBSCRIPTION_NOT_LATEST = 25;

    /**
     * 消费者组主题 订阅配置不存在
     */
    public static final int SUBSCRIPTION_GROUP_NOT_EXIST = 26;

    public static final int FILTER_DATA_NOT_EXIST = 27;

    public static final int FILTER_DATA_NOT_LATEST = 28;

    public static final int TRANSACTION_SHOULD_COMMIT = 200;

    public static final int TRANSACTION_SHOULD_ROLLBACK = 201;

    public static final int TRANSACTION_STATE_UNKNOW = 202;

    public static final int TRANSACTION_STATE_GROUP_WRONG = 203;
    public static final int NO_BUYER_ID = 204;

    public static final int NOT_IN_CURRENT_UNIT = 205;

    public static final int CONSUMER_NOT_ONLINE = 206;

    public static final int CONSUME_MSG_TIMEOUT = 207;

    public static final int NO_MESSAGE = 208;

    public static final int UPDATE_AND_CREATE_ACL_CONFIG_FAILED = 209;

    public static final int DELETE_ACL_CONFIG_FAILED = 210;

    public static final int UPDATE_GLOBAL_WHITE_ADDRS_CONFIG_FAILED = 211;

}
