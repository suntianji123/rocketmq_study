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
package org.apache.rocketmq.common.sysflag;

/**
 * 消息的sysFlag值
 */
public class MessageSysFlag {

    /**
     * 消息的消息体数组经过了压缩处理标志 第1位表示消息是否经过了压缩
     */
    public final static int COMPRESSED_FLAG = 0x1;
    public final static int MULTI_TAGS_FLAG = 0x1 << 1;

    /**
     * 消息没有事务
     */
    public final static int TRANSACTION_NOT_TYPE = 0;

    /**
     * 消息带有事务 消息的第2位表示消息是否带有事务
     */
    public final static int TRANSACTION_PREPARED_TYPE = 0x1 << 2;

    /**
     * 消息事务成功 消息的第4位表示消息的事务成功
     */
    public final static int TRANSACTION_COMMIT_TYPE = 0x2 << 2;

    /**
     * 消息事务失败 消息的第4和第3位表示消息的事务失败
     */
    public final static int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;

    public static int getTransactionValue(final int flag) {
        return flag & TRANSACTION_ROLLBACK_TYPE;
    }

    /**
     * 重置事务状态标志
     * @param flag 原始系统标志值
     * @param type 比较值
     * @return
     */
    public static int resetTransactionValue(final int flag, final int type) {
        return (flag & (~TRANSACTION_ROLLBACK_TYPE)) | type;
    }

    public static int clearCompressedFlag(final int flag) {
        return flag & (~COMPRESSED_FLAG);
    }
}
