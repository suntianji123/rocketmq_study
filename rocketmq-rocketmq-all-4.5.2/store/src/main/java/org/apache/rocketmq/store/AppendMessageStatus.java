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
 * 向mappedFile添加消息返回的状态枚举
 */
public enum AppendMessageStatus {
    PUT_OK,//存储消息成功
    END_OF_FILE,//'消息的总长度超过文件的剩余可写空间时 返回
    MESSAGE_SIZE_EXCEEDED,//消息的超时messageStore限制的值4M时 返回
    PROPERTIES_SIZE_EXCEEDED,//消息头的properties的 属性数量超时Short.Max时返回
    UNKNOWN_ERROR,//未知错误
}
