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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;

public class NamespaceUtil {
    public static final char NAMESPACE_SEPARATOR = '%';
    public static final String STRING_BLANK = "";
    public static final int RETRY_PREFIX_LENGTH = MixAll.RETRY_GROUP_TOPIC_PREFIX.length();
    public static final int DLQ_PREFIX_LENGTH = MixAll.DLQ_GROUP_TOPIC_PREFIX.length();

    /**
     * Unpack namespace from resource, just like:
     * (1) MQ_INST_XX%Topic_XXX --> Topic_XXX
     * (2) %RETRY%MQ_INST_XX%GID_XXX --> %RETRY%GID_XXX
     *
     * @param resourceWithNamespace, topic/groupId with namespace.
     * @return topic/groupId without namespace.
     */
    public static String withoutNamespace(String resourceWithNamespace) {
        if (StringUtils.isEmpty(resourceWithNamespace) || isSystemResource(resourceWithNamespace)) {
            return resourceWithNamespace;
        }

        StringBuffer strBuffer = new StringBuffer();
        if (isRetryTopic(resourceWithNamespace)) {
            strBuffer.append(MixAll.RETRY_GROUP_TOPIC_PREFIX);
        }
        if (isDLQTopic(resourceWithNamespace)) {
            strBuffer.append(MixAll.DLQ_GROUP_TOPIC_PREFIX);
        }

        String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resourceWithNamespace);
        int index = resourceWithoutRetryAndDLQ.indexOf(NAMESPACE_SEPARATOR);
        if (index > 0) {
            String resourceWithoutNamespace = resourceWithoutRetryAndDLQ.substring(index + 1);
            return strBuffer.append(resourceWithoutNamespace).toString();
        }

        return resourceWithNamespace;
    }

    /**
     * If resource contains the namespace, unpack namespace from resource, just like:
     * (1) (MQ_INST_XX1%Topic_XXX1, MQ_INST_XX1) --> Topic_XXX1
     * (2) (MQ_INST_XX2%Topic_XXX2, NULL) --> MQ_INST_XX2%Topic_XXX2
     * (3) (%RETRY%MQ_INST_XX1%GID_XXX1, MQ_INST_XX1) --> %RETRY%GID_XXX1
     * (4) (%RETRY%MQ_INST_XX2%GID_XXX2, MQ_INST_XX3) --> %RETRY%MQ_INST_XX2%GID_XXX2
     *
     * @param resourceWithNamespace, topic/groupId with namespace.
     * @param namespace, namespace to be unpacked.
     * @return topic/groupId without namespace.
     */
    public static String withoutNamespace(String resourceWithNamespace, String namespace) {
        if (StringUtils.isEmpty(resourceWithNamespace) || StringUtils.isEmpty(namespace)) {
            return resourceWithNamespace;
        }

        String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resourceWithNamespace);
        if (resourceWithoutRetryAndDLQ.startsWith(namespace + NAMESPACE_SEPARATOR)) {
            return withoutNamespace(resourceWithNamespace);
        }

        return resourceWithNamespace;
    }

    /**
     * 将原始组名和命名空间名包装
     * @param namespace 命名空间名
     * @param resourceWithOutNamespace 原始名
     * @return 返回一个新的名 //返回 %RETRY%namespace%resouce
     */
    public static String wrapNamespace(String namespace, String resourceWithOutNamespace) {
        //如果没有设置命名空间名
        if (StringUtils.isEmpty(namespace) || StringUtils.isEmpty(resourceWithOutNamespace)) {
            //直接返回资源名
            return resourceWithOutNamespace;
        }

        //如果子系统资源名 不需要包装 或者已经包含命名空间名 直接返回
        if (isSystemResource(resourceWithOutNamespace) || isAlreadyWithNamespace(resourceWithOutNamespace, namespace)) {
            return resourceWithOutNamespace;
        }

        //没有RETRY 和 DLQ的资源ing
        String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resourceWithOutNamespace);

        //实例化一个StringBuilder对象
        StringBuffer strBuffer = new StringBuffer();


        //拼接%RETRY%
        if (isRetryTopic(resourceWithOutNamespace)) {
            strBuffer.append(MixAll.RETRY_GROUP_TOPIC_PREFIX);
        }

        //拼接%DLQ%
        if (isDLQTopic(resourceWithOutNamespace)) {
            strBuffer.append(MixAll.DLQ_GROUP_TOPIC_PREFIX);
        }

        //返回 %RETRY%namespace%resouce
        return strBuffer.append(namespace).append(NAMESPACE_SEPARATOR).append(resourceWithoutRetryAndDLQ).toString();

    }

    /**
     * 判断某个资源名是否已经包装过命名空间名
     * @param resource 资源名
     * @param namespace 命名空间名
     * @return
     */
    public static boolean isAlreadyWithNamespace(String resource, String namespace) {
        //如果命名空间名为空 或者资源名为空 或者是系统资源 返回false
        if (StringUtils.isEmpty(namespace) || StringUtils.isEmpty(resource) || isSystemResource(resource)) {
            return false;
        }

        //去除RETRY DLQ之后的资源ing
        String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resource);

        //返回命名空间 + % + 去掉RETRY和DLQ之后的资源名
        return resourceWithoutRetryAndDLQ.startsWith(namespace + NAMESPACE_SEPARATOR);
    }

    public static String wrapNamespaceAndRetry(String namespace, String consumerGroup) {
        if (StringUtils.isEmpty(consumerGroup)) {
            return null;
        }

        return new StringBuffer()
            .append(MixAll.RETRY_GROUP_TOPIC_PREFIX)
            .append(wrapNamespace(namespace, consumerGroup))
            .toString();
    }

    public static String getNamespaceFromResource(String resource) {
        if (StringUtils.isEmpty(resource) || isSystemResource(resource)) {
            return STRING_BLANK;
        }
        String resourceWithoutRetryAndDLQ = withOutRetryAndDLQ(resource);
        int index = resourceWithoutRetryAndDLQ.indexOf(NAMESPACE_SEPARATOR);

        return index > 0 ? resourceWithoutRetryAndDLQ.substring(0, index) : STRING_BLANK;
    }

    /**
     * 将某个资源名去除retryTopic 和dlq
     * @param originalResource 传入的资源名
     * @return
     */
    private static String withOutRetryAndDLQ(String originalResource) {
        if (StringUtils.isEmpty(originalResource)) {
            return STRING_BLANK;
        }

        //去除%RETRY%
        if (isRetryTopic(originalResource)) {
            return originalResource.substring(RETRY_PREFIX_LENGTH);
        }

        //去除%DLQ%
        if (isDLQTopic(originalResource)) {
            return originalResource.substring(DLQ_PREFIX_LENGTH);
        }

        return originalResource;
    }

    /**
     *
     * 判断某资源名是否为资源名
     * @param resource 传入组名
     * @return
     */
    private static boolean isSystemResource(String resource) {
        if (StringUtils.isEmpty(resource)) {//如果资源名为空
            //返回false
            return false;
        }

        //如果是系统主题名或者系统组名 返回true
        if (MixAll.isSystemTopic(resource) || MixAll.isSysConsumerGroup(resource)) {
            return true;
        }

        //如果是直接创建主题 TBW102 返回true
        return MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC.equals(resource);
    }

    /**
     * 判断某个资源名是否为retry类型的资源名
     * @param resource 资源名
     * @return
     */
    public static boolean isRetryTopic(String resource) {
        //资源名以%RETRY% 表示是retry的资源名
        return StringUtils.isNotBlank(resource) && resource.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
    }

    /**
     * 判断是否为DLO类型的主题
     * @param resource 资源名
     * @return
     */
    public static boolean isDLQTopic(String resource) {
        //资源名以%DLQ%作为前缀
        return StringUtils.isNotBlank(resource) && resource.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX);
    }
}