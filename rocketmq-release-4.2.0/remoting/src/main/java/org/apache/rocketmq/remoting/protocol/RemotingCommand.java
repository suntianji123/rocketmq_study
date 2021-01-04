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
package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 远程命令类
 */
public class RemotingCommand {
    public static final String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
    public static final String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
    public static final String REMOTING_VERSION_KEY = "rocketmq.remoting.version";
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND
    private static final int RPC_ONEWAY = 1; // 0, RPC
    private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP =
        new HashMap<Class<? extends CommandCustomHeader>, Field[]>();
    private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<Class, String>();
    // 1, Oneway
    // 1, RESPONSE_COMMAND
    private static final Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<Field, Boolean>();
    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();

    /**
     * 远程命令版本号 静态字段
     */
    private static volatile int configVersion = -1;
    private static AtomicInteger requestId = new AtomicInteger(0);

    private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

    static {
        final String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
        if (!isBlank(protocol)) {
            try {
                serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("parser specified protocol error. protocol=" + protocol, e);
            }
        }
    }

    /**
     * 请求码
     */
    private int code;

    /**
     * 命令语言 默认为java
     */
    private LanguageCode language = LanguageCode.JAVA;

    /**
     * 远程命令的版本号
     */
    private int version = 0;

    /**
     * 远程命令 id
     */
    private int opaque = requestId.getAndIncrement();
    private int flag = 0;
    private String remark;
    /**
     * 扩展字段
     */
    private HashMap<String, String> extFields;
    //请求头 不会被序列化
    private transient CommandCustomHeader customHeader;

    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

    //不会被序列化
    private transient byte[] body;

    protected RemotingCommand() {
    }

    /**
     * 创建一个远程命令
     * @param code 请求码
     * @param customHeader 自定义的请求头
     * @return
     */
    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        //实例化一个远程命令
        RemotingCommand cmd = new RemotingCommand();
        //设置远程命令的请求码
        cmd.setCode(code);
        //设置远程命令的请求头
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    /**
     * 设置远程命令的版本号
     * @param cmd 远程命令
     */
    private static void setCmdVersion(RemotingCommand cmd) {
        if (configVersion >= 0) {//如果是第一次调用远程命令的静态方法 值为-1
            cmd.setVersion(configVersion);
        } else {
            //获取jvm系统柜参数 获取远程命令版本号 252
            String v = System.getProperty(REMOTING_VERSION_KEY);
            if (v != null) {
                //设置请求版本号
                int value = Integer.parseInt(v);
                //设置请求命令的版本号
                cmd.setVersion(value);
                //设置静态的远程版本号
                configVersion = value;
            }
        }
    }

    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
    }

    public static RemotingCommand createResponseCommand(int code, String remark,
        Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);
        setCmdVersion(cmd);

        if (classHeader != null) {
            try {
                CommandCustomHeader objectHeader = classHeader.newInstance();
                cmd.customHeader = objectHeader;
            } catch (InstantiationException e) {
                return null;
            } catch (IllegalAccessException e) {
                return null;
            }
        }

        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }

    public static RemotingCommand decode(final byte[] array) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }

    /**
     * 解码ByteBuf为RemotingCommand对象
     * @param byteBuffer 需要解码的ByteBuf对象
     * @return
     */
    public static RemotingCommand decode(final ByteBuffer byteBuffer) {
        //获取消息的长度
        int length = byteBuffer.limit();

        //获取头部长度 第4个字节为序列化方式json or rocketmq 前面三个字节表示请求头的长度
        int oriHeaderLen = byteBuffer.getInt();

        //获取请求投的长度
        int headerLength = getHeaderLength(oriHeaderLen);

        //初始化一个长度为请求头长度的字节数组
        byte[] headerData = new byte[headerLength];

        //将请求头数据写入headerData数组
        byteBuffer.get(headerData);

        //反序列化出请求头 生产远程命令行对象
        RemotingCommand cmd = headerDecode(headerData, getProtocolType(oriHeaderLen));

        //获取消息体的长度
        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {//x消息体长度大于0
            bodyData = new byte[bodyLength];
            //从byteBuffer中读出消息
            byteBuffer.get(bodyData);
        }

        //设置命令行的消息体
        cmd.body = bodyData;

        //返回命令行对象
        return cmd;
    }

    /**
     * 获取请求头长度
     * @param length
     * @return
     */
    public static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }

    /**
     * 反序列化请求头数据
     * @param headerData 请求头字节数组
     * @param type 序列化类型
     * @return 返回远程命令行对象
     */
    private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
        switch (type) {//如果序列化方式为json
            case JSON:
                RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
                resultJson.setSerializeTypeCurrentRPC(type);
                return resultJson;
            case ROCKETMQ:
                RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(headerData);
                resultRMQ.setSerializeTypeCurrentRPC(type);
                return resultRMQ;
            default:
                break;
        }

        return null;
    }

    /**
     * 获取序列化协议方式
     * @param source 请求头整数的高8位
     * @return
     */
    public static SerializeType getProtocolType(int source) {
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }

    public static int createNewRequestId() {
        return requestId.incrementAndGet();
    }

    public static SerializeType getSerializeTypeConfigInThisServer() {
        return serializeTypeConfigInThisServer;
    }

    private static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 序列化请求头长度
     * @param source 请求头长度
     * @param type 序列化类型
     * @return
     */
    public static byte[] markProtocolType(int source, SerializeType type) {
        byte[] result = new byte[4];

        //第0个字节表示序列化类型json or rocketmq
        result[0] = type.getCode();

        //1-4个字节表示消息头长度
        result[1] = (byte) ((source >> 16) & 0xFF);
        result[2] = (byte) ((source >> 8) & 0xFF);
        result[3] = (byte) (source & 0xFF);
        return result;
    }

    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    public CommandCustomHeader readCustomHeader() {
        return customHeader;
    }

    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    public CommandCustomHeader decodeCommandCustomHeader(
        Class<? extends CommandCustomHeader> classHeader) throws RemotingCommandException {
        CommandCustomHeader objectHeader;
        try {
            objectHeader = classHeader.newInstance();
        } catch (InstantiationException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        }

        if (this.extFields != null) {

            Field[] fields = getClazzFields(classHeader);
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        try {
                            String value = this.extFields.get(fieldName);
                            if (null == value) {
                                if (!isFieldNullable(field)) {
                                    throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
                                }
                                continue;
                            }

                            field.setAccessible(true);
                            String type = getCanonicalName(field.getType());
                            Object valueParsed;

                            if (type.equals(STRING_CANONICAL_NAME)) {
                                valueParsed = value;
                            } else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                                valueParsed = Integer.parseInt(value);
                            } else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                                valueParsed = Long.parseLong(value);
                            } else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                                valueParsed = Boolean.parseBoolean(value);
                            } else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                                valueParsed = Double.parseDouble(value);
                            } else {
                                throw new RemotingCommandException("the custom field <" + fieldName + "> type is not supported");
                            }

                            field.set(objectHeader, valueParsed);

                        } catch (Throwable e) {
                            log.error("Failed field [{}] decoding", fieldName, e);
                        }
                    }
                }
            }

            objectHeader.checkFields();
        }

        return objectHeader;
    }

    private Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        Field[] field = CLASS_HASH_MAP.get(classHeader);

        if (field == null) {
            field = classHeader.getDeclaredFields();
            synchronized (CLASS_HASH_MAP) {
                CLASS_HASH_MAP.put(classHeader, field);
            }
        }
        return field;
    }

    private boolean isFieldNullable(Field field) {
        if (!NULLABLE_FIELD_CACHE.containsKey(field)) {
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (NULLABLE_FIELD_CACHE) {
                NULLABLE_FIELD_CACHE.put(field, annotation == null);
            }
        }
        return NULLABLE_FIELD_CACHE.get(field);
    }

    private String getCanonicalName(Class clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);

        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }

    public ByteBuffer encode() {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData = this.headerEncode();
        length += headerData.length;

        // 3> body data length
        if (this.body != null) {
            length += body.length;
        }

        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // length
        result.putInt(length);

        // header length
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        // body data;
        if (this.body != null) {
            result.put(this.body);
        }

        result.flip();

        return result;
    }

    /**
     * 将远程命令的头部编码为字节数组
     * @return
     */
    private byte[] headerEncode() {
        //将customerHeader中的所有字段值放入extFields集合
        this.makeCustomHeaderToNet();
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            return RocketMQSerializable.rocketMQProtocolEncode(this);
        } else {
            //将整个对象以json格式序列化为字节数组
            return RemotingSerializable.encode(this);
        }
    }


    /**
     * 将自定义的customerheader中的所有字段值放入extFields集合
     */
    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {//自定义的消息头不为空
            //获取自定义消息头的所有的字段
            Field[] fields = getClazzFields(customHeader.getClass());
            if (null == this.extFields) {
                this.extFields = new HashMap<String, String>();
            }

            //遍历用户自定义消息头的所有字段
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {//过滤掉静态字段
                    String name = field.getName();
                    if (!name.startsWith("this")) {//过滤掉this字段
                        Object value = null;
                        try {
                            //设置字段可以访问
                            field.setAccessible(true);
                            //获取字段的值
                            value = field.get(this.customHeader);
                        } catch (Exception e) {
                            log.error("Failed to access field [{}]", name, e);
                        }

                        //将值放入扩展字段集合
                        if (value != null) {
                            this.extFields.put(name, value.toString());
                        }
                    }
                }
            }
        }
    }

    /**
     * 编码远程命令的请求头
     * @return
     */
    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    /**
     * 编码消息头
     * @param bodyLength 消息体的长度
     * @return
     */
    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size
        int length = 4;//保留消息的总长度值

        // 2> header data length
        byte[] headerData;
        //将customerheader中的字段值放入extFields集合后  将整个对象是哟红json序列化字节数组返回
        headerData = this.headerEncode();

        //消息的总长度
        length += headerData.length;

        // 加上消息体的长度
        length += bodyLength;

        //分配一个堆内缓冲区 0-3字节 消息的总长度 4 表示序列化类型 5-7表示请求头长度
        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // 写入总长度 0-3
        result.putInt(length);

        // 写入序列化类型 请求头长度4-7
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // 写入请求头字节数组
        result.put(headerData);

        //重置position的值
        result.flip();

        return result;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }

    public int getCode() {
        return code;
    }

    /**
     * 设置远程命令的请求码
     * @param code 请求码
     */
    public void setCode(int code) {
        this.code = code;
    }

    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    /**
     * 设置远程命令的版本号 默认为252
     * @param version 版本号
     */
    public void setVersion(int version) {
        this.version = version;
    }

    /**
     * 获取远程命令id
     * @return
     */
    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<String, String>();
        }
        extFields.put(key, value);
    }

    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
            + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
            + serializeTypeCurrentRPC + "]";
    }

    public SerializeType getSerializeTypeCurrentRPC() {
        return serializeTypeCurrentRPC;
    }

    public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
        this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
    }
}