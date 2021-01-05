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
package org.apache.rocketmq.common.filter;

import java.net.URL;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public class FilterAPI {
    public static URL classFile(final String className) {
        final String javaSource = simpleClassName(className) + ".java";
        URL url = FilterAPI.class.getClassLoader().getResource(javaSource);
        return url;
    }

    public static String simpleClassName(final String className) {
        String simple = className;
        int index = className.lastIndexOf(".");
        if (index >= 0) {
            simple = className.substring(index + 1);
        }

        return simple;
    }

    /**
     * 根据消费者组 主题 子主题表达式构造一个订阅对象
     * 子主题表达式解析出多个标签 设置到标签集合 将每一个标签的hash值 放入哈希值集合
     * @param consumerGroup 消费者组名
     * @param topic 主题
     * @param subString 子主题表达式
     * @return
     * @throws Exception
     */
    public static SubscriptionData buildSubscriptionData(final String consumerGroup, String topic,
        String subString) throws Exception {
        //实例化一个订阅数据对象
        SubscriptionData subscriptionData = new SubscriptionData();
        //设置订阅数据对象的主题
        subscriptionData.setTopic(topic);
        //设置订阅数据对象的子主题表达式
        subscriptionData.setSubString(subString);

        if (null == subString || subString.equals(SubscriptionData.SUB_ALL) || subString.length() == 0) {//设置订阅所有的主题
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
        } else {
            //分割子主题表达式 返回多个子主题表达式
            String[] tags = subString.split("\\|\\|");
            if (tags.length > 0) {//子主题表达式的长度大于0
                for (String tag : tags) {//遍历子主题
                    if (tag.length() > 0) {//如果长度大于0
                        String trimString = tag.trim();//去除空格
                        if (trimString.length() > 0) {//出去空格之后的长度大有0
                            //设置订阅的标签
                            subscriptionData.getTagsSet().add(trimString);
                            //设置订阅标签的hashcode值集合
                            subscriptionData.getCodeSet().add(trimString.hashCode());
                        }
                    }
                }
            } else {
                throw new Exception("subString split error");
            }
        }

        //返回订阅data
        return subscriptionData;
    }

    public static SubscriptionData build(final String topic, final String subString,
        final String type) throws Exception {
        if (ExpressionType.TAG.equals(type) || type == null) {
            return buildSubscriptionData(null, topic, subString);
        }

        if (subString == null || subString.length() < 1) {
            throw new IllegalArgumentException("Expression can't be null! " + type);
        }

        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        subscriptionData.setExpressionType(type);

        return subscriptionData;
    }
}
