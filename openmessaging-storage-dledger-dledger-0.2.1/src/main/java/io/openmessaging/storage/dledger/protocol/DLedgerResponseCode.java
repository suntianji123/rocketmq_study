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

package io.openmessaging.storage.dledger.protocol;

import java.util.HashMap;
import java.util.Map;

public enum DLedgerResponseCode {

    UNKNOWN(-1, ""),
    SUCCESS(200, ""),
    TIMEOUT(300, ""),
    METADATA_ERROR(301, ""),

    /**
     * 远程rpc server响应码 网络错误（远程服务器连接不上）
     */
    NETWORK_ERROR(302, ""),
    UNSUPPORTED(303, ""),
    UNKNOWN_GROUP(400, ""),

    /**
     * 某个节点id不是集群下的成员节点返回这个错误
     */
    UNKNOWN_MEMBER(401, ""),

    /**
     * 发送请求的leaderId是对端节点的id
     */
    UNEXPECTED_MEMBER(402, ""),
    /**
     * 过期的轮次
     */
    EXPIRED_TERM(403, ""),
    NOT_LEADER(404, ""),
    NOT_FOLLOWER(405, ""),
    INCONSISTENT_STATE(406, ""),
    INCONSISTENT_TERM(407, ""),
    INCONSISTENT_INDEX(408, ""),

    /**
     * 对端节点存在leaderId 但是发送心跳请求的leaderId与对端节点的leaderId不一致
     */
    INCONSISTENT_LEADER(409, ""),
    INDEX_OUT_OF_RANGE(410, ""),
    UNEXPECTED_ARGUMENT(411, ""),
    REPEATED_REQUEST(412, ""),
    REPEATED_PUSH(413, ""),
    DISK_ERROR(414, ""),

    /**
     * 磁盘满了
     */
    DISK_FULL(415, ""),

    /**
     * 对端节点的状态机轮次大于发送心跳的leader 对端节点会立刻进入候选人状态 并进行拉票
     */
    TERM_NOT_READY(416, ""),
    FALL_BEHIND_TOO_MUCH(417, ""),
    TAKE_LEADERSHIP_FAILED(418, ""),
    INDEX_LESS_THAN_LOCAL_BEGIN(419, ""),
    REQUEST_WITH_EMPTY_BODYS(420, ""),
    INTERNAL_ERROR(500, ""),

    /**
     * 主节点同步添加消息给其他节点 异步操作的轮次小于主节点的轮次 轮次改变
     */
    TERM_CHANGED(501, ""),
    WAIT_QUORUM_ACK_TIMEOUT(502, ""),
    LEADER_PENDING_FULL(503, ""),
    ILLEGAL_MEMBER_STATE(504, ""),
    LEADER_NOT_READY(505, ""),
    LEADER_TRANSFERRING(506, "");

    private static Map<Integer, DLedgerResponseCode> codeMap = new HashMap<>();

    static {
        for (DLedgerResponseCode responseCode : DLedgerResponseCode.values()) {
            codeMap.put(responseCode.code, responseCode);
        }
    }

    private int code;
    private String desc;

    DLedgerResponseCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static DLedgerResponseCode valueOf(int code) {
        DLedgerResponseCode tmp = codeMap.get(code);
        if (tmp != null) {
            return tmp;
        } else {
            return UNKNOWN;
        }

    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    @Override
    public String toString() {
        return String.format("[code=%d,name=%s,desc=%s]", code, name(), desc);
    }
}
