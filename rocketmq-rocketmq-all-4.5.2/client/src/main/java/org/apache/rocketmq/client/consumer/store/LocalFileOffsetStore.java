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
package org.apache.rocketmq.client.consumer.store;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 本地的存放广播站消费队列偏移量类
 */
public class LocalFileOffsetStore implements OffsetStore {
    public final static String LOCAL_OFFSET_STORE_DIR = System.getProperty(
        "rocketmq.client.localOffsetStoreDir",
        System.getProperty("user.home") + File.separator + ".rocketmq_offsets");
    private final static InternalLogger log = ClientLogger.getLog();
    /**
     * mqclientinstance
     */
    private final MQClientInstance mQClientFactory;

    /**
     * 消费者组名
     */
    private final String groupName;

    /**
     * 存储消费者组下所有主题消息队列消费偏移量的文件的基本路径./store/.rocketmq_offsets/192.168.0.1@33/consumeGroupname/offsets.json
     */
    private final String storePath;

    /**
     * 存储当前消费者组主题消息队列偏移量的列表
     */
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>();

    /**
     * 实例化一个存放广播站消费队列偏移量
     * @param mQClientFactory mqclientinstace
     * @param groupName 消费者组名
     */
    public LocalFileOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        //设置mqclientinstance
        this.mQClientFactory = mQClientFactory;
        //设置消费者组名
        this.groupName = groupName;
        //本地存储偏移量文件的路径
        this.storePath = LOCAL_OFFSET_STORE_DIR + File.separator +
            this.mQClientFactory.getClientId() + File.separator +
            this.groupName + File.separator +
            "offsets.json";
    }

    /**
     * 从本地offsets.json文件中加载消费者组订阅的所有主题消息队列偏移量
     * @throws MQClientException
     */
    @Override
    public void load() throws MQClientException {
        //获取当前消费者组下所有的主题消息队列对应的偏移量列表对象
        OffsetSerializeWrapper offsetSerializeWrapper = this.readLocalOffset();
        if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
            //将从本地json文件加载出的主题消息队列偏移量列表保存起来
            offsetTable.putAll(offsetSerializeWrapper.getOffsetTable());

            //遍历所有的主题消息队列
            for (MessageQueue mq : offsetSerializeWrapper.getOffsetTable().keySet()) {
                //获取主题消息队列的偏移量
                AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                //将主题消息队列对应的偏移量打印出来
                log.info("load consumer's offset, {} {} {}",
                    this.groupName,
                    mq,
                    offset.get());
            }
        }
    }

    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            AtomicLong offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    offsetOld.set(offset);
                }
            }
        }
    }

    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                case READ_FROM_STORE: {
                    OffsetSerializeWrapper offsetSerializeWrapper;
                    try {
                        offsetSerializeWrapper = this.readLocalOffset();
                    } catch (MQClientException e) {
                        return -1;
                    }
                    if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
                        AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                        if (offset != null) {
                            this.updateOffset(mq, offset.get(), false);
                            return offset.get();
                        }
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;

        OffsetSerializeWrapper offsetSerializeWrapper = new OffsetSerializeWrapper();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            if (mqs.contains(entry.getKey())) {
                AtomicLong offset = entry.getValue();
                offsetSerializeWrapper.getOffsetTable().put(entry.getKey(), offset);
            }
        }

        String jsonString = offsetSerializeWrapper.toJson(true);
        if (jsonString != null) {
            try {
                MixAll.string2File(jsonString, this.storePath);
            } catch (IOException e) {
                log.error("persistAll consumer offset Exception, " + this.storePath, e);
            }
        }
    }

    @Override
    public void persist(MessageQueue mq) {
    }

    @Override
    public void removeOffset(MessageQueue mq) {

    }

    @Override
    public void updateConsumeOffsetToBroker(final MessageQueue mq, final long offset, final boolean isOneway)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());

        }
        return cloneOffsetTable;
    }

    /**
     * 从本地offsets.json中加载消费者组所有的主题消息队列偏移量
     * @return
     * @throws MQClientException
     */
    private OffsetSerializeWrapper readLocalOffset() throws MQClientException {
        String content = null;
        try {
            //读取json文件中的内容
            content = MixAll.file2String(this.storePath);
        } catch (IOException e) {
            log.warn("Load local offset store file exception", e);
        }
        if (null == content || content.length() == 0) {
            return this.readLocalOffsetBak();
        } else {
            //将json字符串反序列化为主题消息队列对应的偏移量
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                offsetSerializeWrapper =
                    OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception, and try to correct", e);
                return this.readLocalOffsetBak();
            }

            return offsetSerializeWrapper;
        }
    }

    private OffsetSerializeWrapper readLocalOffsetBak() throws MQClientException {
        String content = null;
        try {
            content = MixAll.file2String(this.storePath + ".bak");
        } catch (IOException e) {
            log.warn("Load local offset store bak file exception", e);
        }
        if (content != null && content.length() > 0) {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                offsetSerializeWrapper =
                    OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception", e);
                throw new MQClientException("readLocalOffset Exception, maybe fastjson version too low"
                    + FAQUrl.suggestTodo(FAQUrl.LOAD_JSON_EXCEPTION),
                    e);
            }
            return offsetSerializeWrapper;
        }

        return null;
    }
}
