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

package org.apache.rocketmq.common.utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;

/**
 * 轻量的Http客户端类
 */
public class HttpTinyClient {

    /**
     * 发送http get请求
     * @param url 请求路径
     * @param headers 请求头
     * @param paramValues 请求参数
     * @param encoding 编码格式
     * @param readTimeoutMs 请求超时是哪
     * @return
     * @throws IOException
     */
    static public HttpResult httpGet(String url, List<String> headers, List<String> paramValues,
        String encoding, long readTimeoutMs) throws IOException {

        //编码请求参数
        String encodedContent = encodingParams(paramValues, encoding);
        //将请求参数拼接到url之后
        url += (null == encodedContent) ? "" : ("?" + encodedContent);

        //定义一个httpurlconnection
        HttpURLConnection conn = null;
        try {
            //根据url创建一个连接
            conn = (HttpURLConnection) new URL(url).openConnection();
            //设置conn的请求方法为get
            conn.setRequestMethod("GET");
            //设置连接超时实际那
            conn.setConnectTimeout((int) readTimeoutMs);
            //设置读取数据超时事假
            conn.setReadTimeout((int) readTimeoutMs);
            //设置请求头 指定编码格式
            setHeaders(conn, headers, encoding);

            //开始连接
            conn.connect();
            //获取响应码
            int respCode = conn.getResponseCode();
            String resp = null;

            //响应码为200
            if (HttpURLConnection.HTTP_OK == respCode) {
                //获取响应字符串
                resp = IOTinyUtils.toString(conn.getInputStream(), encoding);
            } else {
                resp = IOTinyUtils.toString(conn.getErrorStream(), encoding);
            }

            //实例化一个Http结果对象
            return new HttpResult(respCode, resp);
        } finally {
            if (conn != null) {
                //关闭conn连接
                conn.disconnect();
            }
        }
    }

    /**
     * 编码请求参数
     * @param paramValues 请求参数列表
     * @param encoding 编码字符集格式
     * @return
     * @throws UnsupportedEncodingException
     */
    static private String encodingParams(List<String> paramValues, String encoding)
        throws UnsupportedEncodingException {
        //实例化一个StringBuilder对象
        StringBuilder sb = new StringBuilder();
        if (null == paramValues) {//如果请求参数列表为null 直接返回
            return null;
        }

        //获取请求参数的迭代器
        for (Iterator<String> iter = paramValues.iterator(); iter.hasNext(); ) {
            //拼接参数名
            sb.append(iter.next()).append("=");
            //拼接参数值
            sb.append(URLEncoder.encode(iter.next(), encoding));
            //拼接&符号
            if (iter.hasNext()) {
                sb.append("&");
            }
        }
        //返回请求参数解析后的字符串
        return sb.toString();
    }

    static private void setHeaders(HttpURLConnection conn, List<String> headers, String encoding) {
        if (null != headers) {
            for (Iterator<String> iter = headers.iterator(); iter.hasNext(); ) {
                conn.addRequestProperty(iter.next(), iter.next());
            }
        }
        conn.addRequestProperty("Client-Version", MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));
        conn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + encoding);

        String ts = String.valueOf(System.currentTimeMillis());
        conn.addRequestProperty("Metaq-Client-RequestTS", ts);
    }

    /**
     * @return the http response of given http post request
     */
    static public HttpResult httpPost(String url, List<String> headers, List<String> paramValues,
        String encoding, long readTimeoutMs) throws IOException {

        //编码请求参数
        String encodedContent = encodingParams(paramValues, encoding);

        //定义个httpurlconnection连接
        HttpURLConnection conn = null;
        try {
            //实例化一个连接对象
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(3000);
            conn.setReadTimeout((int) readTimeoutMs);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            setHeaders(conn, headers, encoding);

            conn.getOutputStream().write(encodedContent.getBytes(MixAll.DEFAULT_CHARSET));

            int respCode = conn.getResponseCode();
            String resp = null;


            if (HttpURLConnection.HTTP_OK == respCode) {
                resp = IOTinyUtils.toString(conn.getInputStream(), encoding);
            } else {
                resp = IOTinyUtils.toString(conn.getErrorStream(), encoding);
            }
            return new HttpResult(respCode, resp);
        } finally {
            if (null != conn) {
                conn.disconnect();
            }
        }
    }

    /**
     * HttpResult http响应结果类
     */
    static public class HttpResult {
        //响应码
        final public int code;
        //响应内容
        final public String content;

        /**
         * 实例化一个HttpResult对象
         * @param code 响应码
         * @param content 响应内容字符串
         */
        public HttpResult(int code, String content) {
            this.code = code;
            this.content = content;
        }
    }
}
