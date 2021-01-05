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
package org.apache.rocketmq.common;

import java.io.IOException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 配置管理器类
 */
public abstract class ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public abstract String encode();

    /**
     * //加载配置文件 反序列化 获取其中的主题配置列表 设置到topicConfigManager的主题配置列表
     * 加载配置文件 返回是否成功加载配置文件
     * @return
     */
    public boolean load() {

        //定义一个文件名字符串
        String fileName = null;
        try {

            //获取配置文件路径 C:\Users\Administrator\store\config\topics.json
            fileName = this.configFilePath();

            //以字符串的形式 返回文件路径文件中的内容
            String jsonString = MixAll.file2String(fileName);

            //如果文件没有内容
            if (null == jsonString || jsonString.length() == 0) {
                //加载副本文件
                return this.loadBak();
            } else {
                //解码加载的字符串
                this.decode(jsonString);
                log.info("load {} OK", fileName);
                return true;
            }
        } catch (Exception e) {
            log.error("load [{}] failed, and try to load backup file", fileName, e);
            return this.loadBak();
        }
    }

    /**
     * 获取配置文件路径
     */
    public abstract String configFilePath();

    /**
     * 第一次访问配置文件不存在
     * @return
     */
    private boolean loadBak() {
        String fileName = null;
        try {
            //虎丘配置文件名
            fileName = this.configFilePath();

            //判断副本文件是否存在
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load [{}] OK", fileName);
                return true;
            }
        } catch (Exception e) {
            log.error("load [{}] Failed", fileName, e);
            return false;
        }

        return true;
    }

    public abstract void decode(final String jsonString);

    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file [{}] exception", fileName, e);
            }
        }
    }

    public abstract String encode(final boolean prettyFormat);
}
