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
package org.apache.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;

/**
 *mq client管理类
 */
public class MQClientManager {
    private final static Logger log = ClientLogger.getLog();

    /**
     * mq client管理对象 单例
     */
    private static MQClientManager instance = new MQClientManager();

    /**
     * mq client实例下载factoryTable中的下标
     */
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();

    /**
     * netty client 列表 key:本机ip@进程pid
     */
    private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable =
        new ConcurrentHashMap<String, MQClientInstance>();

    private MQClientManager() {

    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig) {
        return getAndCreateMQClientInstance(clientConfig, null);
    }

    /**
     * 创建或者一个netty client对象
     * @param clientConfig 客户端配置对象
     * @param rpcHook 钓
     * @return
     */
    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        //根据客户端配置的 clientip(ipv4地址) instanceName(pid值) 创建netty client id clientip@instanceName
        String clientId = clientConfig.buildMQClientId();

        //从客户端列表中根据clientip获取netty client实例
        MQClientInstance instance = this.factoryTable.get(clientId);
        if (null == instance) {//实例为空

            //创建一个mqclient实例
            instance =
                new MQClientInstance(clientConfig.cloneClientConfig(),
                    this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            //如果执行存在clientid的实例 则不进行存放 返回之前已经存放的mqclient实例
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {//之前已经向mqclienttablez鸿放入过这个clientid的mqclient实例
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        //返回这个clientid的mqclient实例
        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
