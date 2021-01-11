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
 * $Id: TopAddressing.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.namesrv;

import java.io.IOException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.utils.HttpTinyClient;

public class TopAddressing {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String nsAddr;
    private String wsAddr;
    private String unitName;

    public TopAddressing(final String wsAddr) {
        this(wsAddr, null);
    }

    public TopAddressing(final String wsAddr, final String unitName) {
        this.wsAddr = wsAddr;
        this.unitName = unitName;
    }

    /**
     * 获取指定字符串的第一行字符串
     * @param str 指定字符串
     * @return
     */
    private static String clearNewLine(final String str) {

        //去除字符串空格
        String newString = str.trim();
        //获取制表符下标
        int index = newString.indexOf("\r");
        if (index != -1) {
            //截取制表符之前的字符串
            return newString.substring(0, index);
        }

        //获取换行符的下标
        index = newString.indexOf("\n");
        if (index != -1) {
            //截取换行符之前的字符串
            return newString.substring(0, index);
        }

        //返回字符串
        return newString;
    }

    /**
     * 从netty server获取中心服务器地址
     * @return
     */
    public final String fetchNSAddr() {
        return fetchNSAddr(true, 3000);
    }

    /**
     * 从web server获取中心服务器地址
     * @param verbose 是否为冗长的
     * @param timeoutMills 请求web server超时时间
     * @return
     */
    public final String fetchNSAddr(boolean verbose, long timeoutMills) {
        //web server地址
        String url = this.wsAddr;
        try {
            //单元名
            if (!UtilAll.isBlank(this.unitName)) {
                url = url + "-" + this.unitName + "?nofix=1";
            }

            //发送http请求
            HttpTinyClient.HttpResult result = HttpTinyClient.httpGet(url, null, null, "UTF-8", timeoutMills);
            //请求ok
            if (200 == result.code) {
                //获取响应结果
                String responseStr = result.content;
                if (responseStr != null) {
                    //返回第一行数据
                    return clearNewLine(responseStr);
                } else {
                    log.error("fetch nameserver address is null");
                }
            } else {
                log.error("fetch nameserver address failed. statusCode=" + result.code);
            }
        } catch (IOException e) {
            if (verbose) {
                log.error("fetch name server address exception", e);
            }
        }

        if (verbose) {
            String errorMsg =
                "connect to " + url + " failed, maybe the domain name " + MixAll.getWSAddr() + " not bind in /etc/hosts";
            errorMsg += FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL);

            log.warn(errorMsg);
        }
        return null;
    }

    public String getNsAddr() {
        return nsAddr;
    }

    public void setNsAddr(String nsAddr) {
        this.nsAddr = nsAddr;
    }
}
