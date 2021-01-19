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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

public class IndexService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * Maximum times to attempt index file creation.
     */
    private static final int MAX_TRY_IDX_CREATE = 3;
    private final DefaultMessageStore defaultMessageStore;

    /**
     * 哈希槽号 最大值为500w
     */
    private final int hashSlotNum;

    /**
     * 最大下标数量 最大为500w * 4
     */
    private final int indexNum;

    /**
     * 根目录
     */
    private final String storePath;
    private final ArrayList<IndexFile> indexFileList = new ArrayList<IndexFile>();

    /**
     * 读写锁
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * 实例化一个Index服务
     * @param store
     */
    public IndexService(final DefaultMessageStore store) {
        //指定消息存储服务
        this.defaultMessageStore = store;
        //设置哈希槽值
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        //设置最大下标数量
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        //设置indexFile文件的根目录
        this.storePath =
            StorePathConfigHelper.getStorePathIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }

    public boolean load(final boolean lastExitOK) {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    f.load();

                    if (!lastExitOK) {
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint()
                            .getIndexMsgTimestamp()) {
                            f.destroy(0);
                            continue;
                        }
                    }

                    log.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    log.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }

            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) {
            List<IndexFile> fileList = new ArrayList<IndexFile>();
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    fileList.add(f);
                } else {
                    break;
                }
            }

            this.deleteExpiredFile(fileList);
        }
    }

    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                    boolean destroyed = file.destroy(3000);
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<Long>(maxNum);

        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    IndexFile f = this.indexFileList.get(i - 1);
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    if (f.isTimeMatched(begin, end)) {

                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end, lastFile);
                    }

                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    /**
     * 索引关键字
     * @param topic 主题
     * @param key 消息id
     * @return
     */
    private String buildKey(final String topic, final String key) {
        //返回 主题#消息id
        return topic + "#" + key;
    }

    /**
     * 将消息的uniod和key分别计算哈希值 计算消息在commitlog中的位置信息在indexFile的位置
     * @param req 消息的分发请求
     */
    public void buildIndex(DispatchRequest req) {
        //获取或者创建一个IndexFile文件
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {
            //获取结束的物理偏移量
            long endPhyOffset = indexFile.getEndPhyOffset();
            //获取request
            DispatchRequest msg = req;
            //获取主题
            String topic = msg.getTopic();
            //获取keys
            String keys = msg.getKeys();

            //如果消息的物理偏移量 小于已经写到的物理偏移量
            if (msg.getCommitLogOffset() < endPhyOffset) {
                return;
            }

            //获取事务类型
            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }

            //获取消息的唯一id
            if (req.getUniqKey() != null) {
                //将消息的索引信息写入到indexFile文件
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                if (indexFile == null) {
                    log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                    return;
                }
            }

            if (keys != null && keys.length() > 0) {
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                        //将消息的关键字写入到indexFile 用于查询
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        if (indexFile == null) {
                            log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                            return;
                        }
                    }
                }
            }
        } else {
            log.error("build index error, stop building index");
        }
    }

    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            log.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");

            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }

            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

        return indexFile;
    }

    /**
     * 创建消息的下标文件
     * @return
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        //重试次数
        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            //获取或者创建一个IndexFile
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile)
                break;

            try {
                log.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }

        if (null == indexFile) {
            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            log.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    /**
     * 获取获取创建indexFile
     * @return
     */
    public IndexFile getAndCreateLastIndexFile() {
        //indexFile
        IndexFile indexFile = null;
        //上一个indexFile
        IndexFile prevIndexFile = null;
        //indexFile记录的最后一条索引信息的在commitlog消息的物理偏移量
        long lastUpdateEndPhyOffset = 0;
        //indexFile记录的最后一条索引信息在commitlog的消息的生产日期
        long lastUpdateIndexTimestamp = 0;

        {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull()) {
                    //没有写满
                    indexFile = tmp;
                } else {
                    //最后一个IndexFile文件写满了 新创建的IndexFile的最后一次更新的物理偏移 还有更新的时间戳
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();

                    //前一个IndexFile
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }

        if (indexFile == null) {
            try {
                //获取indexFile 文件名
                String fileName =
                    this.storePath + File.separator
                        + UtilAll.timeMillisToHumanString(System.currentTimeMillis());

                //实例化一个indexFile
                indexFile =
                    new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                        lastUpdateIndexTimestamp);
                this.readWriteLock.writeLock().lock();
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                log.error("getLastIndexFile exception ", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }

            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;

                //开启一个守护线程 刷新前一个indexFile
                Thread flushThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        IndexService.this.flush(flushThisFile);
                    }
                }, "FlushIndexFileThread");

                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    /**
     * 将某个indexFile的mappedFile中的内容刷新到磁盘
     * @param f
     */
    public void flush(final IndexFile f) {
        if (null == f)
            return;

        long indexMsgTimestamp = 0;

        if (f.isWriteFull()) {//文件已经写满
            indexMsgTimestamp = f.getEndTimestamp();
        }

        //刷新文件到磁盘
        f.flush();

        if (indexMsgTimestamp > 0) {//文件写满了
            //记录一个时间点
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            //刷新记录点
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {

    }

    public void shutdown() {

    }
}
