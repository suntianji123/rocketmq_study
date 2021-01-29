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

package io.openmessaging.storage.dledger.entry;

import java.nio.ByteBuffer;

public class DLedgerEntryCoder {

    /**
     * 将消息实体编码写入bytebuffer
     * @param entry 消息实体
     * @param byteBuffer 临时bytebuffer
     */
    public static void encode(DLedgerEntry entry, ByteBuffer byteBuffer) {
        //清理bytebuffer
        byteBuffer.clear();
        //获取消息的占用的总的字节数
        int size = entry.computSizeInBytes();
        //写入消息的模数
        byteBuffer.putInt(entry.getMagic());
        //写入消息占的总的字节数
        byteBuffer.putInt(size);
        //写入消息的index值
        byteBuffer.putLong(entry.getIndex());
        //写入消息的轮次
        byteBuffer.putLong(entry.getTerm());
        //写入消息在commitlog中的偏移量
        byteBuffer.putLong(entry.getPos());
        //写入消息的通道编号
        byteBuffer.putInt(entry.getChannel());
        //写入消息的链crc值
        byteBuffer.putInt(entry.getChainCrc());
        //写入消息体curc值
        byteBuffer.putInt(entry.getBodyCrc());
        //写入消息体长度
        byteBuffer.putInt(entry.getBody().length);
        //写入消息体
        byteBuffer.put(entry.getBody());
        //将bytebuffer由写改为读模式
        byteBuffer.flip();
    }

    /**
     * 将消息的索引写入到某个bytebuffer对象
     * @param pos 消息的偏移量
     * @param size 消息大小所占字节数
     * @param magic 消息的模数
     * @param index 消息的下标
     * @param term 消息的轮次
     * @param byteBuffer 目标bytebuffer对象
     */
    public static void encodeIndex(long pos, int size, int magic, long index, long term, ByteBuffer byteBuffer) {
        //清理目标bytebuffer对象
        byteBuffer.clear();
        //写入模数
        byteBuffer.putInt(magic);
        //写入消息的偏移量
        byteBuffer.putLong(pos);
        //写入消息所占的字节数
        byteBuffer.putInt(size);
        //写入消息的index值
        byteBuffer.putLong(index);
        //写入消息的轮次
        byteBuffer.putLong(term);
        //将bytebuffer由写改为读模式
        byteBuffer.flip();
    }

    /**
     * 解码消息实体
     * @param byteBuffer 存储消息信息的缓存区
     * @return
     */
    public static DLedgerEntry decode(ByteBuffer byteBuffer) {
        return decode(byteBuffer, true);
    }

    /**
     * 解码缓存区字节数组为消息实体
     * @param byteBuffer 缓存区
     * @param readBody 是否将消息体也读取
     * @return
     */
    public static DLedgerEntry decode(ByteBuffer byteBuffer, boolean readBody) {
        //实例化一个消息实体
        DLedgerEntry entry = new DLedgerEntry();
        //设置消息的模数
        entry.setMagic(byteBuffer.getInt());
        //设置消息所占的字节数
        entry.setSize(byteBuffer.getInt());
        //设置消息的索引值
        entry.setIndex(byteBuffer.getLong());
        //设置消息的轮数
        entry.setTerm(byteBuffer.getLong());
        //设置消息的偏移量
        entry.setPos(byteBuffer.getLong());
        //设置消息的通道
        entry.setChannel(byteBuffer.getInt());
        //设置消息链 crc值
        entry.setChainCrc(byteBuffer.getInt());
        //设置消息体的crc值
        entry.setBodyCrc(byteBuffer.getInt());
        //获取消息体大小
        int bodySize = byteBuffer.getInt();
        if (readBody && bodySize < entry.getSize()) {
            //获取消息体
            byte[] body = new byte[bodySize];
            byteBuffer.get(body);
            entry.setBody(body);
        }
        return entry;
    }

    /**
     * 将消息的在mappedFile list中的偏移量写入bytebuffer
     * @param byteBuffer 缓存区
     * @param pos 偏移量
     */
    public static void setPos(ByteBuffer byteBuffer, long pos) {
        //标记消息的写位置
        byteBuffer.mark();
        //移动写的位置
        byteBuffer.position(byteBuffer.position() + DLedgerEntry.POS_OFFSET);
        //写入消息的偏移量
        byteBuffer.putLong(pos);
        //重新设置消息的写位置
        byteBuffer.reset();
    }

    public static long getPos(ByteBuffer byteBuffer) {
        long pos;
        byteBuffer.mark();
        byteBuffer.position(byteBuffer.position() + DLedgerEntry.POS_OFFSET);
        pos = byteBuffer.getLong();
        byteBuffer.reset();
        return pos;
    }

    /**
     * 将消息的index,轮次，模式写入bytebuffer缓存区
     * @param byteBuffer 缓存区
     * @param index index值
     * @param term 轮次
     * @param magic 模数
     */
    public static void setIndexTerm(ByteBuffer byteBuffer, long index, long term, int magic) {
        //记录之前的位置
        byteBuffer.mark();
        //向缓存区中写入模式
        byteBuffer.putInt(magic);
        //移动写的起始位置
        byteBuffer.position(byteBuffer.position() + 4);
        //写入消息的index
        byteBuffer.putLong(index);
        //写入消息的轮次
        byteBuffer.putLong(term);
        //重置bytebuffer的写位置
        byteBuffer.reset();
    }

}
