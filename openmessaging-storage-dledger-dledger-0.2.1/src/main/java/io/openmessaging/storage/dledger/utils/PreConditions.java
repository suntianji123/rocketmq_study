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

package io.openmessaging.storage.dledger.utils;

import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;

/**
 * 前置条件 检测
 */
public class PreConditions {

    public static void check(boolean expression, DLedgerResponseCode code) throws DLedgerException {
        check(expression, code, null);
    }

    /**
     * 检测
     * @param expression 检测表达式
     * @param code 错误码
     * @param message 错误信息
     * @throws DLedgerException
     */
    public static void check(boolean expression, DLedgerResponseCode code, String message) throws DLedgerException {
        if (!expression) {//表达式的值为false
            message = message == null ? code.toString()
                : code.toString() + " " + message;
            throw new DLedgerException(code, message);
        }
    }

    public static void check(boolean expression, DLedgerResponseCode code, String format,
        Object... args) throws DLedgerException {
        if (!expression) {
            String message = code.toString() + " " + String.format(format, args);
            throw new DLedgerException(code, message);
        }
    }
}
