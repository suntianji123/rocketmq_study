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
package org.apache.rocketmq.common.message;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageDecoder {
    public final static int MSG_ID_LENGTH = 8 + 8;

    public final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");
    public final static int MESSAGE_MAGIC_CODE_POSTION = 4;
    public final static int MESSAGE_FLAG_POSTION = 16;
    public final static int MESSAGE_PHYSIC_OFFSET_POSTION = 28;
    public final static int MESSAGE_STORE_TIMESTAMP_POSTION = 56;
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    public static final char NAME_VALUE_SEPARATOR = 1;
    public static final char PROPERTY_SEPARATOR = 2;
    public static final int PHY_POS_POSITION =  4 + 4 + 4 + 4 + 4 + 8;
    public static final int BODY_SIZE_POSITION = 4 // 1 TOTALSIZE
        + 4 // 2 MAGICCODE
        + 4 // 3 BODYCRC
        + 4 // 4 QUEUEID
        + 4 // 5 FLAG
        + 8 // 6 QUEUEOFFSET
        + 8 // 7 PHYSICALOFFSET
        + 4 // 8 SYSFLAG
        + 8 // 9 BORNTIMESTAMP
        + 8 // 10 BORNHOST
        + 8 // 11 STORETIMESTAMP
        + 8 // 12 STOREHOSTADDRESS
        + 4 // 13 RECONSUMETIMES
        + 8; // 14 Prepared Transaction Offset

    /**
     * 创建消息id
     * @param input 存储消息id的ByteBuffer对象
     * @param addr 消息存储的服务器的地址的ByteBuffer对象
     * @param offset 消息在整个commitLog文件系统中偏移量
     * @return
     */
    public static String createMessageId(final ByteBuffer input, final ByteBuffer addr, final long offset) {
        //将读变为写
        input.flip();
        //设置可写的最大字节长度
        input.limit(MessageDecoder.MSG_ID_LENGTH);

        //写入地址
        input.put(addr);
        //写入消息在整个文件夹系统中的偏移量
        input.putLong(offset);

        //将storeHost和起始偏移量转为id
        return UtilAll.bytes2string(input.array());
    }

    public static String createMessageId(SocketAddress socketAddress, long transactionIdhashCode) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        byteBuffer.put(inetSocketAddress.getAddress().getAddress());
        byteBuffer.putInt(inetSocketAddress.getPort());
        byteBuffer.putLong(transactionIdhashCode);
        byteBuffer.flip();
        return UtilAll.bytes2string(byteBuffer.array());
    }

    public static MessageId decodeMessageId(final String msgId) throws UnknownHostException {
        SocketAddress address;
        long offset;

        byte[] ip = UtilAll.string2bytes(msgId.substring(0, 8));
        byte[] port = UtilAll.string2bytes(msgId.substring(8, 16));
        ByteBuffer bb = ByteBuffer.wrap(port);
        int portInt = bb.getInt(0);
        address = new InetSocketAddress(InetAddress.getByAddress(ip), portInt);

        // offset
        byte[] data = UtilAll.string2bytes(msgId.substring(16, 32));
        bb = ByteBuffer.wrap(data);
        offset = bb.getLong(0);

        return new MessageId(address, offset);
    }

    /**
     * Just decode properties from msg buffer.
     *
     * @param byteBuffer msg commit log buffer.
     */
    public static Map<String, String> decodeProperties(java.nio.ByteBuffer byteBuffer) {
        int topicLengthPosition = BODY_SIZE_POSITION + 4 + byteBuffer.getInt(BODY_SIZE_POSITION);

        byte topicLength = byteBuffer.get(topicLengthPosition);

        short propertiesLength = byteBuffer.getShort(topicLengthPosition + 1 + topicLength);

        byteBuffer.position(topicLengthPosition + 1 + topicLength + 2);

        if (propertiesLength > 0) {
            byte[] properties = new byte[propertiesLength];
            byteBuffer.get(properties);
            String propertiesString = new String(properties, CHARSET_UTF8);
            Map<String, String> map = string2messageProperties(propertiesString);
            return map;
        }
        return null;
    }

    public static MessageExt decode(java.nio.ByteBuffer byteBuffer) {
        return decode(byteBuffer, true, true, false);
    }

    public static MessageExt clientDecode(java.nio.ByteBuffer byteBuffer, final boolean readBody) {
        return decode(byteBuffer, readBody, true, true);
    }

    public static MessageExt decode(java.nio.ByteBuffer byteBuffer, final boolean readBody) {
        return decode(byteBuffer, readBody, true, false);
    }

    public static byte[] encode(MessageExt messageExt, boolean needCompress) throws Exception {
        byte[] body = messageExt.getBody();
        byte[] topics = messageExt.getTopic().getBytes(CHARSET_UTF8);
        byte topicLen = (byte) topics.length;
        String properties = messageProperties2String(messageExt.getProperties());
        byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
        short propertiesLength = (short) propertiesBytes.length;
        int sysFlag = messageExt.getSysFlag();
        byte[] newBody = messageExt.getBody();
        if (needCompress && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
            newBody = UtilAll.compress(body, 5);
        }
        int bodyLength = newBody.length;
        int storeSize = messageExt.getStoreSize();
        ByteBuffer byteBuffer;
        if (storeSize > 0) {
            byteBuffer = ByteBuffer.allocate(storeSize);
        } else {
            storeSize = 4 // 1 TOTALSIZE
                + 4 // 2 MAGICCODE
                + 4 // 3 BODYCRC
                + 4 // 4 QUEUEID
                + 4 // 5 FLAG
                + 8 // 6 QUEUEOFFSET
                + 8 // 7 PHYSICALOFFSET
                + 4 // 8 SYSFLAG
                + 8 // 9 BORNTIMESTAMP
                + 8 // 10 BORNHOST
                + 8 // 11 STORETIMESTAMP
                + 8 // 12 STOREHOSTADDRESS
                + 4 // 13 RECONSUMETIMES
                + 8 // 14 Prepared Transaction Offset
                + 4 + bodyLength // 14 BODY
                + 1 + topicLen // 15 TOPIC
                + 2 + propertiesLength // 16 propertiesLength
                + 0;
            byteBuffer = ByteBuffer.allocate(storeSize);
        }
        // 1 TOTALSIZE
        byteBuffer.putInt(storeSize);

        // 2 MAGICCODE
        byteBuffer.putInt(MESSAGE_MAGIC_CODE);

        // 3 BODYCRC
        int bodyCRC = messageExt.getBodyCRC();
        byteBuffer.putInt(bodyCRC);

        // 4 QUEUEID
        int queueId = messageExt.getQueueId();
        byteBuffer.putInt(queueId);

        // 5 FLAG
        int flag = messageExt.getFlag();
        byteBuffer.putInt(flag);

        // 6 QUEUEOFFSET
        long queueOffset = messageExt.getQueueOffset();
        byteBuffer.putLong(queueOffset);

        // 7 PHYSICALOFFSET
        long physicOffset = messageExt.getCommitLogOffset();
        byteBuffer.putLong(physicOffset);

        // 8 SYSFLAG
        byteBuffer.putInt(sysFlag);

        // 9 BORNTIMESTAMP
        long bornTimeStamp = messageExt.getBornTimestamp();
        byteBuffer.putLong(bornTimeStamp);

        // 10 BORNHOST
        InetSocketAddress bornHost = (InetSocketAddress) messageExt.getBornHost();
        byteBuffer.put(bornHost.getAddress().getAddress());
        byteBuffer.putInt(bornHost.getPort());

        // 11 STORETIMESTAMP
        long storeTimestamp = messageExt.getStoreTimestamp();
        byteBuffer.putLong(storeTimestamp);

        // 12 STOREHOST
        InetSocketAddress serverHost = (InetSocketAddress) messageExt.getStoreHost();
        byteBuffer.put(serverHost.getAddress().getAddress());
        byteBuffer.putInt(serverHost.getPort());

        // 13 RECONSUMETIMES
        int reconsumeTimes = messageExt.getReconsumeTimes();
        byteBuffer.putInt(reconsumeTimes);

        // 14 Prepared Transaction Offset
        long preparedTransactionOffset = messageExt.getPreparedTransactionOffset();
        byteBuffer.putLong(preparedTransactionOffset);

        // 15 BODY
        byteBuffer.putInt(bodyLength);
        byteBuffer.put(newBody);

        // 16 TOPIC
        byteBuffer.put(topicLen);
        byteBuffer.put(topics);

        // 17 properties
        byteBuffer.putShort(propertiesLength);
        byteBuffer.put(propertiesBytes);

        return byteBuffer.array();
    }

    /**
     * 解码消息
     * @param byteBuffer bytebuffer对象
     * @param readBody 是否解码消息体
     * @param deCompressBody 是否解压消息体
     * @return
     */
    public static MessageExt decode(
        java.nio.ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody) {
        return decode(byteBuffer, readBody, deCompressBody, false);
    }

    /**
     * 解码消息
     * @param byteBuffer 存有消息字节数组的bytebuffer对象
     * @param readBody 是否解码消息体
     * @param deCompressBody 是否解压消息体
     * @param isClient 是否为客户端的消息
     * @return
     */
    public static MessageExt decode(
        java.nio.ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody, final boolean isClient) {
        try {

            MessageExt msgExt;
            if (isClient) {
                msgExt = new MessageClientExt();
            } else {
                //实例化一个消息
                msgExt = new MessageExt();
            }

            // 1 获取消息总大小
            int storeSize = byteBuffer.getInt();
            //设置存在于commitlog中的总大小
            msgExt.setStoreSize(storeSize);

            //获取魔数
            byteBuffer.getInt();

            // 获取crc值
            int bodyCRC = byteBuffer.getInt();
            //设置crc值
            msgExt.setBodyCRC(bodyCRC);

            // 4 消息所在的主题队列编号
            int queueId = byteBuffer.getInt();
            //设置消息所在的主题队列编号
            msgExt.setQueueId(queueId);

            // 5 获取生产者自定义的标志
            int flag = byteBuffer.getInt();
            //设置消息自定义的标志
            msgExt.setFlag(flag);

            // 6 获取消息在主题队列中的偏移量
            long queueOffset = byteBuffer.getLong();
            //设置消息在主题队列中的偏移量
            msgExt.setQueueOffset(queueOffset);

            // 7 获取消息在commitlog中的偏移量
            long physicOffset = byteBuffer.getLong();
            //设置消息在主题队列中的偏移量
            msgExt.setCommitLogOffset(physicOffset);

            // 8 获取系统标志
            int sysFlag = byteBuffer.getInt();
            //设置系统标志
            msgExt.setSysFlag(sysFlag);

            // 9 获取生产者消息的时间
            long bornTimeStamp = byteBuffer.getLong();
            //设置生产消息的时间
            msgExt.setBornTimestamp(bornTimeStamp);

            // 10 获取生产者的地址
            byte[] bornHost = new byte[4];
            //设置生产者地址
            byteBuffer.get(bornHost, 0, 4);
            //虎丘生产者端口号
            int port = byteBuffer.getInt();
            //设置生产者地址
            msgExt.setBornHost(new InetSocketAddress(InetAddress.getByAddress(bornHost), port));

            // 11 获取存储消息的时间
            long storeTimestamp = byteBuffer.getLong();
            //设置存储消息的时间
            msgExt.setStoreTimestamp(storeTimestamp);

            // 12 获取存储消息的地址
            byte[] storeHost = new byte[4];
            byteBuffer.get(storeHost, 0, 4);
            port = byteBuffer.getInt();
            //设置存储消息的地址
            msgExt.setStoreHost(new InetSocketAddress(InetAddress.getByAddress(storeHost), port));

            // 13 获取重新消费的次数
            int reconsumeTimes = byteBuffer.getInt();
            //设置重新消费的次数
            msgExt.setReconsumeTimes(reconsumeTimes);

            // 14 Prepared Transaction Offset
            long preparedTransactionOffset = byteBuffer.getLong();
            msgExt.setPreparedTransactionOffset(preparedTransactionOffset);

            // 15 BODY
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byte[] body = new byte[bodyLen];
                    byteBuffer.get(body);

                    // uncompress body
                    if (deCompressBody && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
                        body = UtilAll.uncompress(body);
                    }

                    //设置消息体
                    msgExt.setBody(body);
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            // 16 TOPIC
            byte topicLen = byteBuffer.get();
            byte[] topic = new byte[(int) topicLen];
            byteBuffer.get(topic);
            //设置主题
            msgExt.setTopic(new String(topic, CHARSET_UTF8));

            // 17 properies map属性的数量
            short propertiesLength = byteBuffer.getShort();
            if (propertiesLength > 0) {
                byte[] properties = new byte[propertiesLength];
                byteBuffer.get(properties);
                String propertiesString = new String(properties, CHARSET_UTF8);
                Map<String, String> map = string2messageProperties(propertiesString);
                //设置properties map属性
                msgExt.setProperties(map);
            }

            ByteBuffer byteBufferMsgId = ByteBuffer.allocate(MSG_ID_LENGTH);
            //消息id
            String msgId = createMessageId(byteBufferMsgId, msgExt.getStoreHostBytes(), msgExt.getCommitLogOffset());
            //设置消息id
            msgExt.setMsgId(msgId);

            if (isClient) {
                ((MessageClientExt) msgExt).setOffsetMsgId(msgId);
            }

            return msgExt;
        } catch (Exception e) {
            byteBuffer.position(byteBuffer.limit());
        }

        return null;
    }

    public static List<MessageExt> decodes(java.nio.ByteBuffer byteBuffer) {
        return decodes(byteBuffer, true);
    }

    public static List<MessageExt> decodes(java.nio.ByteBuffer byteBuffer, final boolean readBody) {
        List<MessageExt> msgExts = new ArrayList<MessageExt>();
        while (byteBuffer.hasRemaining()) {
            MessageExt msgExt = clientDecode(byteBuffer, readBody);
            if (null != msgExt) {
                msgExts.add(msgExt);
            } else {
                break;
            }
        }
        return msgExts;
    }

    /**
     * 将properties map中的属性值转为字符串
     * @param properties map
     * @return
     */
    public static String messageProperties2String(Map<String, String> properties) {
        //实例化一个StringBuilder对象
        StringBuilder sb = new StringBuilder();
        if (properties != null) {//map不为空
            for (final Map.Entry<String, String> entry : properties.entrySet()) {//遍历map
                //属性名
                final String name = entry.getKey();

                //属性值
                final String value = entry.getValue();

                //拼接属性名
                sb.append(name);
                sb.append(NAME_VALUE_SEPARATOR);

                //拼接属性值
                sb.append(value);
                sb.append(PROPERTY_SEPARATOR);
            }
        }

        //返回属性值
        return sb.toString();
    }

    public static Map<String, String> string2messageProperties(final String properties) {
        Map<String, String> map = new HashMap<String, String>();
        if (properties != null) {
            String[] items = properties.split(String.valueOf(PROPERTY_SEPARATOR));
            for (String i : items) {
                String[] nv = i.split(String.valueOf(NAME_VALUE_SEPARATOR));
                if (2 == nv.length) {
                    map.put(nv[0], nv[1]);
                }
            }
        }

        return map;
    }

    public static byte[] encodeMessage(Message message) {
        //only need flag, body, properties
        byte[] body = message.getBody();
        int bodyLen = body.length;
        String properties = messageProperties2String(message.getProperties());
        byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
        //note properties length must not more than Short.MAX
        short propertiesLength = (short) propertiesBytes.length;
        int sysFlag = message.getFlag();
        int storeSize = 4 // 1 TOTALSIZE
            + 4 // 2 MAGICCOD
            + 4 // 3 BODYCRC
            + 4 // 4 FLAG
            + 4 + bodyLen // 4 BODY
            + 2 + propertiesLength;
        ByteBuffer byteBuffer = ByteBuffer.allocate(storeSize);
        // 1 TOTALSIZE
        byteBuffer.putInt(storeSize);

        // 2 MAGICCODE
        byteBuffer.putInt(0);

        // 3 BODYCRC
        byteBuffer.putInt(0);

        // 4 FLAG
        int flag = message.getFlag();
        byteBuffer.putInt(flag);

        // 5 BODY
        byteBuffer.putInt(bodyLen);
        byteBuffer.put(body);

        // 6 properties
        byteBuffer.putShort(propertiesLength);
        byteBuffer.put(propertiesBytes);

        return byteBuffer.array();
    }

    public static Message decodeMessage(ByteBuffer byteBuffer) throws Exception {
        Message message = new Message();

        // 1 TOTALSIZE
        byteBuffer.getInt();

        // 2 MAGICCODE
        byteBuffer.getInt();

        // 3 BODYCRC
        byteBuffer.getInt();

        // 4 FLAG
        int flag = byteBuffer.getInt();
        message.setFlag(flag);

        // 5 BODY
        int bodyLen = byteBuffer.getInt();
        byte[] body = new byte[bodyLen];
        byteBuffer.get(body);
        message.setBody(body);

        // 6 properties
        short propertiesLen = byteBuffer.getShort();
        byte[] propertiesBytes = new byte[propertiesLen];
        byteBuffer.get(propertiesBytes);
        message.setProperties(string2messageProperties(new String(propertiesBytes, CHARSET_UTF8)));

        return message;
    }

    public static byte[] encodeMessages(List<Message> messages) {
        //TO DO refactor, accumulate in one buffer, avoid copies
        List<byte[]> encodedMessages = new ArrayList<byte[]>(messages.size());
        int allSize = 0;
        for (Message message : messages) {
            byte[] tmp = encodeMessage(message);
            encodedMessages.add(tmp);
            allSize += tmp.length;
        }
        byte[] allBytes = new byte[allSize];
        int pos = 0;
        for (byte[] bytes : encodedMessages) {
            System.arraycopy(bytes, 0, allBytes, pos, bytes.length);
            pos += bytes.length;
        }
        return allBytes;
    }

    public static List<Message> decodeMessages(ByteBuffer byteBuffer) throws Exception {
        //TO DO add a callback for processing,  avoid creating lists
        List<Message> msgs = new ArrayList<Message>();
        while (byteBuffer.hasRemaining()) {
            Message msg = decodeMessage(byteBuffer);
            msgs.add(msg);
        }
        return msgs;
    }
}
