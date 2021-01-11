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
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;

/**
 * 分配mappedFile的服务类
 */
public class AllocateMappedFileService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 等待获取分配mappedFile的结果超时时间
     */
    private static int waitTimeOut = 1000 * 5;

    /**
     * 正在分配的mappedFile的请求集合 文件路径|请求对象
     */
    private ConcurrentMap<String, AllocateRequest> requestTable =
        new ConcurrentHashMap<String, AllocateRequest>();

    /**
     * 等待分配mappedFile的请求队列
     */
    private PriorityBlockingQueue<AllocateRequest> requestQueue =
        new PriorityBlockingQueue<AllocateRequest>();

    /**
     * 分配mappedFile的服务出现了异常
     */
    private volatile boolean hasException = false;
    private DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * 发送创建mappedFile请求
     * @param nextFilePath 请求mappedFile的全路径
     * @param nextNextFilePath 下下个mappedFile的全路径
     * @param fileSize 文件大小
     * @return
     */
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        //可以提交的请求数
        int canSubmitRequests = 2;

        //启动临时存储池
        if (this.messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) { //if broker is slave, don't fast fail even no buffer in pool
                canSubmitRequests = this.messageStore.getTransientStorePool().availableBufferNums() - this.requestQueue.size();
            }
        }

        //实例化一个分配mappedFile的请求对象
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);

        //将分配mappedFile的请求放入请求列表 之前不能存在
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;

        if (nextPutOK) {//之前不存在
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                this.requestTable.remove(nextFilePath);
                return null;
            }

            //向等待分配mappedFile的队列提交一个分配请求
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {//提交任务失败
                log.warn("never expected here, add a request to preallocate queue failed");
            }

            //可以提交的请求数量减-
            canSubmitRequests--;
        }

        //继续实例化一个分配下下个mappedFile的请求
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        //分配下下个mappedFile的请求在请求列表中不存在
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {//可以分配
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                this.requestTable.remove(nextNextFilePath);
            } else {
                //向分配mappedFile的任务队列提交分配下下个mappedFile的请求
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {//没有提交成功
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }

        if (hasException) {//如果服务运行出现了异常
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        //再次获取分配请求对象
        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                //阻塞当前 地等待获取分配结果
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {//分配失败
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {//分配成功
                    this.requestTable.remove(nextFilePath);
                    //获取分配的mappedFile文件
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    @Override
    public String getServiceName() {
        return AllocateMappedFileService.class.getSimpleName();
    }

    @Override
    public void shutdown() {
        super.shutdown(true);
        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mappedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mappedFile.getFileName());
                req.mappedFile.destroy(1000);
            }
        }
    }

    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped() && this.mmapOperation()) {

        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * 分配mappedFile的操作流程
     * @return
     */
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            req = this.requestQueue.take();
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (null == expectedRequest) {//从请求任务列表 获取请求任务 没有请求任务
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize());
                return true;
            }
            if (expectedRequest != req) {//任务不同
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
                return true;
            }

            if (req.getMappedFile() == null) {//这个请求还没有分配mappedFile
                //分配开始时间
                long beginTime = System.currentTimeMillis();

                //定义一个mappedFile
                MappedFile mappedFile;
                if (messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {//如果临时存储池启用
                    try {
                        //从serverLoader加载MappedFile对象
                        mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();

                        //初始化一个mappedFile文件
                        mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    } catch (RuntimeException e) {
                        log.warn("Use default implementation.");
                        mappedFile = new MappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    }
                } else {
                    //实例化一个mappedFile文件
                    mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                }

                //已经过去的时间
                long elapsedTime = UtilAll.computeElapsedTimeMilliseconds(beginTime);
                if (elapsedTime > 10) {//超过了10毫秒
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + elapsedTime + " queue size " + queueSize
                        + " " + req.getFilePath() + " " + req.getFileSize());
                }

                // pre write mappedFile
                if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig()
                    .getMappedFileSizeCommitLog() && this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                    mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                        this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                }

                //设置mappedFile
                req.setMappedFile(mappedFile);
                this.hasException = false;

                //请求成功
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");

            //设置有异常
            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            if (null != req) {
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            if (req != null && isSuccess)//减少计数器
                req.getCountDownLatch().countDown();
        }
        return true;
    }

    /**
     * 分配MappedFile的请求类
     */
    static class AllocateRequest implements Comparable<AllocateRequest> {

        /**
         * 文件全路径
         */
        private String filePath;

        /**
         * 文件大小
         */
        private int fileSize;
        private CountDownLatch countDownLatch = new CountDownLatch(1);

        /**
         * 请求分配的mappedFile
         */
        private volatile MappedFile mappedFile = null;

        /**
         * 实例化一个分配mappedFile对象
         * @param filePath 文件路径
         * @param fileSize 文件最大大小
         */
        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }

        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }

        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize)
                return 1;
            else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                if (mName < oName) {
                    return -1;
                } else if (mName > oName) {
                    return 1;
                } else {
                    return 0;
                }
            }
            // return this.fileSize < other.fileSize ? 1 : this.fileSize >
            // other.fileSize ? -1 : 0;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
            result = prime * result + fileSize;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null)
                    return false;
            } else if (!filePath.equals(other.filePath))
                return false;
            if (fileSize != other.fileSize)
                return false;
            return true;
        }
    }
}
