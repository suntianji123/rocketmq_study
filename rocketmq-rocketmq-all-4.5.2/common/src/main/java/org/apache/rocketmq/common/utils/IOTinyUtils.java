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

import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * IOTinyUtils类
 */
public class IOTinyUtils {

    /**
     * 将输入流按照指定编码格式转为字符串
     * @param input 输入流
     * @param encoding 编码格式
     * @return
     * @throws IOException
     */
    static public String toString(InputStream input, String encoding) throws IOException {
        return (null == encoding) ? toString(new InputStreamReader(input, RemotingHelper.DEFAULT_CHARSET)) : toString(new InputStreamReader(
            input, encoding));
    }

    /**
     * 将Reader中的字节以字符串的形式返回
     * @param reader Reader对象
     * @return
     * @throws IOException
     */
    static public String toString(Reader reader) throws IOException {
        //实例化一个CharArrayWriter对象
        CharArrayWriter sw = new CharArrayWriter();
        copy(reader, sw);
        //返回写对象的字符串
        return sw.toString();
    }

    /**
     * 将reader字符数组写入writer对象
     * @param input reader对象
     * @param output writer对象
     * @return
     * @throws IOException
     */
    static public long copy(Reader input, Writer output) throws IOException {
        //实例化一个字符串
        char[] buffer = new char[1 << 12];
        //写入的字符数量
        long count = 0;
        for (int n = 0; (n = input.read(buffer)) >= 0; ) {
            //一次最大读取buffer.size个字符
            output.write(buffer, 0, n);
            count += n;
        }

        //返回写入到writer中的字符数量
        return count;
    }

    static public List<String> readLines(Reader input) throws IOException {
        BufferedReader reader = toBufferedReader(input);
        List<String> list = new ArrayList<String>();
        String line;
        for (; ; ) {
            line = reader.readLine();
            if (null != line) {
                list.add(line);
            } else {
                break;
            }
        }
        return list;
    }

    static private BufferedReader toBufferedReader(Reader reader) {
        return reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
    }

    static public void copyFile(String source, String target) throws IOException {
        File sf = new File(source);
        if (!sf.exists()) {
            throw new IllegalArgumentException("source file does not exist.");
        }
        File tf = new File(target);
        tf.getParentFile().mkdirs();
        if (!tf.exists() && !tf.createNewFile()) {
            throw new RuntimeException("failed to create target file.");
        }

        FileChannel sc = null;
        FileChannel tc = null;
        try {
            tc = new FileOutputStream(tf).getChannel();
            sc = new FileInputStream(sf).getChannel();
            sc.transferTo(0, sc.size(), tc);
        } finally {
            if (null != sc) {
                sc.close();
            }
            if (null != tc) {
                tc.close();
            }
        }
    }

    public static void delete(File fileOrDir) throws IOException {
        if (fileOrDir == null) {
            return;
        }

        if (fileOrDir.isDirectory()) {
            cleanDirectory(fileOrDir);
        }

        fileOrDir.delete();
    }

    public static void cleanDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }

        File[] files = directory.listFiles();
        if (files == null) { // null if security restricted
            throw new IOException("Failed to list contents of " + directory);
        }

        IOException exception = null;
        for (File file : files) {
            try {
                delete(file);
            } catch (IOException ioe) {
                exception = ioe;
            }
        }

        if (null != exception) {
            throw exception;
        }
    }

    public static void writeStringToFile(File file, String data, String encoding) throws IOException {
        OutputStream os = null;
        try {
            os = new FileOutputStream(file);
            os.write(data.getBytes(encoding));
        } finally {
            if (null != os) {
                os.close();
            }
        }
    }
}
