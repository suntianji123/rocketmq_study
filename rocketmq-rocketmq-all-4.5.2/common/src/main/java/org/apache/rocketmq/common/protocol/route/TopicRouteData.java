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
 * $Id: TopicRouteData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * 主题发布路径数据类
 * 多个广播站会为当前主题 分配多个不同数量的消息队列来存储广播站的消息 主题的一条消息只会存在于一个广播站的一个主题消息队列
 */
public class TopicRouteData extends RemotingSerializable {

    /**
     * 如果主题消息是顺序的 顺序主题配置 brokerName1:10;brokerName2:20
     * 表示哪些广播站是顺序广播这个主题消息的 :之后的数字表示消息队列的数量
     * brokerName1广播站广播10之后 -> brokerName2再次广播20次
     */
    private String orderTopicConf;

    /**
     * 每个广播站为主题的消息分配的读写队列数量配置
     */
    private List<QueueData> queueDatas;

    /**
     * 可以写入这个主题的消息的广播站数据列表
     */
    private List<BrokerData> brokerDatas;

    /**
     * 过滤某个广播站地址 从指定的列表中选择一个广播站地址
     */
    private HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    /**
     * 克隆某个主题路径信息对象
     * @return
     */
    public TopicRouteData cloneTopicRouteData() {
        //实例化一个主题路径信息对象
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setQueueDatas(new ArrayList<QueueData>());
        topicRouteData.setBrokerDatas(new ArrayList<BrokerData>());
        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        topicRouteData.setOrderTopicConf(this.orderTopicConf);

        if (this.queueDatas != null) {//不同广播站对该主题的队列列表不为null
            topicRouteData.getQueueDatas().addAll(this.queueDatas);
        }

        if (this.brokerDatas != null) {//广播站列表
            topicRouteData.getBrokerDatas().addAll(this.brokerDatas);
        }

        if (this.filterServerTable != null) {//过滤服务器列表
            topicRouteData.getFilterServerTable().putAll(this.filterServerTable);
        }

        return topicRouteData;
    }

    public List<QueueData> getQueueDatas() {
        return queueDatas;
    }

    public void setQueueDatas(List<QueueData> queueDatas) {
        this.queueDatas = queueDatas;
    }

    public List<BrokerData> getBrokerDatas() {
        return brokerDatas;
    }

    public void setBrokerDatas(List<BrokerData> brokerDatas) {
        this.brokerDatas = brokerDatas;
    }

    public HashMap<String, List<String>> getFilterServerTable() {
        return filterServerTable;
    }

    public void setFilterServerTable(HashMap<String, List<String>> filterServerTable) {
        this.filterServerTable = filterServerTable;
    }

    public String getOrderTopicConf() {
        return orderTopicConf;
    }

    public void setOrderTopicConf(String orderTopicConf) {
        this.orderTopicConf = orderTopicConf;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerDatas == null) ? 0 : brokerDatas.hashCode());
        result = prime * result + ((orderTopicConf == null) ? 0 : orderTopicConf.hashCode());
        result = prime * result + ((queueDatas == null) ? 0 : queueDatas.hashCode());
        result = prime * result + ((filterServerTable == null) ? 0 : filterServerTable.hashCode());
        return result;
    }

    /**
     * 判断两个主题路径信息对象是否内容一样
     * @param obj 老的
     * @return
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TopicRouteData other = (TopicRouteData) obj;
        if (brokerDatas == null) {
            if (other.brokerDatas != null)
                return false;
        } else if (!brokerDatas.equals(other.brokerDatas))
            return false;
        if (orderTopicConf == null) {
            if (other.orderTopicConf != null)
                return false;
        } else if (!orderTopicConf.equals(other.orderTopicConf))
            return false;
        if (queueDatas == null) {
            if (other.queueDatas != null)
                return false;
        } else if (!queueDatas.equals(other.queueDatas))
            return false;
        if (filterServerTable == null) {
            if (other.filterServerTable != null)
                return false;
        } else if (!filterServerTable.equals(other.filterServerTable))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "TopicRouteData [orderTopicConf=" + orderTopicConf + ", queueDatas=" + queueDatas
            + ", brokerDatas=" + brokerDatas + ", filterServerTable=" + filterServerTable + "]";
    }
}
