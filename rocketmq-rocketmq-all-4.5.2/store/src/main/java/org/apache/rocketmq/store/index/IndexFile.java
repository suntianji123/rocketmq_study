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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * indexFile类
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;

    /**
     * 哈希槽值
     */
    private final int hashSlotNum;

    /**
     * 下标数量
     */
    private final int indexNum;

    /**
     * mappedFile文件
     */
    private final MappedFile mappedFile;

    /**
     * 文件通道
     */
    private final FileChannel fileChannel;

    /**
     * 文件的byteBuffer对象
     */
    private final MappedByteBuffer mappedByteBuffer;

    /**
     * 头部
     */
    private final IndexHeader indexHeader;

    /**
     * 实例化一个indexFile对象
     * @param fileName 文件名
     * @param hashSlotNum 最大哈希槽值
     * @param indexNum 最大下标值数量
     * @param endPhyOffset  结束的物理偏移量
     * @param endTimestamp 上次写入下标信息的时间戳
     * @throws IOException
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        //文件总的大小
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        //实例化一个mappedFile文件
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        //获取fileChannel
        this.fileChannel = this.mappedFile.getFileChannel();
        //获取bytebuffer
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        //设置哈希槽值
        this.hashSlotNum = hashSlotNum;
        //设置下标数量
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    /**
     * 将indexFile的mappedByteBuffer字节刷新到磁盘
     */
    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            //更新bytebuffer的前面40个字节
            this.indexHeader.updateByteBuffer();
            //刷新bytebuffer
            this.mappedByteBuffer.force();
            //释放mappedFile
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }


    /**
     * 向indexFile中写入索引 Header 20个字节 + 索引的位置（哈希槽位置写入索引信息的位置）+ 索引信息
     * @param key 索引关键字 topic#msgId
     * @param phyOffset 对应于commitlog中消息的物理偏移量
     * @param storeTimestamp 消息生产的时间
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {//如果indexFile中已经写入的索引的数量超过最大值
            //获取索引的哈希值
            int keyHash = indexKeyHashMethod(key);
            //获取索引的哈希值槽位
            int slotPos = keyHash % this.hashSlotNum;

            //获取消息的索引信息实际存放的位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                //获取当前索引在indexFile中的位置 第n个索引
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }
                //生产时间 - 开始写入indexFile的时间
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                //获取时间
                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                //索引存放的起始位置
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                //在索引起始位置写入4个字节的哈希值
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);

                //然后再写入8个字节的消息在commitlog中的偏移量
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);

                //然后再4个字节写入时间差值
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                //在写入4个字节的哈希值所在的槽下标
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                //在哈希槽位写入 索引信息的位置
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    //设置beginPhyOffset对应commitlog中的偏移量
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    //开始时间 对应于消息的存储时间
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                //增减已经用掉的哈希槽数量
                this.indexHeader.incHashSlotCount();

                //增加已经写入索引信息的数量
                this.indexHeader.incIndexCount();

                //设置最后一次将消息的索引信息写入到indexFile 对应于消息在commitlog中的偏移量
                this.indexHeader.setEndPhyOffset(phyOffset);
                //设置最后一次将消息的索引信息写入indexFile 记录消息的生产时间
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    /**
     * 索引key的哈希值
     * @param key 索引key topic#msgId
     * @return
     */
    public int indexKeyHashMethod(final String key) {
        //获取索引的哈希值
        int keyHash = key.hashCode();
        //取绝对值
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    /**
     * 获取已经写到的物理偏移量（对应于commitlog中的物理偏移量）
     * @return
     */
    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
