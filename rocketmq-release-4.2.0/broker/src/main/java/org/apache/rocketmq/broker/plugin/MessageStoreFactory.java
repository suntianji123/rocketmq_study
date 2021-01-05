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

package org.apache.rocketmq.broker.plugin;

import java.io.IOException;
import java.lang.reflect.Constructor;
import org.apache.rocketmq.store.MessageStore;

/**
 *消息存储工厂类
 */
public final class MessageStoreFactory {

    /**
     * 构建消息存储对象
     * @param context 消息存储插件上下文
     * @param messageStore 消息存储对象
     * @return
     * @throws IOException
     */
    public final static MessageStore build(MessageStorePluginContext context, MessageStore messageStore)
        throws IOException {
        //获取消息存储插件名
        String plugin = context.getBrokerConfig().getMessageStorePlugIn();
        if (plugin != null && plugin.trim().length() != 0) {
            String[] pluginClasses = plugin.split(",");//解析插件名
            for (int i = pluginClasses.length - 1; i >= 0; --i) {
                String pluginClass = pluginClasses[i];//遍历单个插件名
                try {
                    //通过反射获取插件的class对象
                    @SuppressWarnings("unchecked")
                    Class<AbstractPluginMessageStore> clazz = (Class<AbstractPluginMessageStore>) Class.forName(pluginClass);
                    //调用插件的构造方法
                    Constructor<AbstractPluginMessageStore> construct = clazz.getConstructor(MessageStorePluginContext.class, MessageStore.class);
                    //实例化插件 设置messageStore的相关属性
                    messageStore = construct.newInstance(context, messageStore);
                } catch (Throwable e) {
                    throw new RuntimeException(String.format(
                        "Initialize plugin's class %s not found!", pluginClass), e);
                }
            }
        }

        //返回messageStore
        return messageStore;
    }
}
