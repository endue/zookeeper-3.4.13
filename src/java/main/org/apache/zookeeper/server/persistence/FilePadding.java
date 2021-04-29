/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FilePadding {
    private static final Logger LOG;
    // 预分配大小64M
    private static long preAllocSize = 65536 * 1024;
    private static final ByteBuffer fill = ByteBuffer.allocateDirect(1);

    static {
        LOG = LoggerFactory.getLogger(FileTxnLog.class);

        String size = System.getProperty("zookeeper.preAllocSize");
        if (size != null) {
            try {
                preAllocSize = Long.parseLong(size) * 1024;
            } catch (NumberFormatException e) {
                LOG.warn(size + " is not a valid value for preAllocSize");
            }
        }
    }
    // 当前文件预分配的字节数
    private long currentSize;

    /**
     * Getter of preAllocSize has been added for testing
     */
    public static long getPreAllocSize() {
        return preAllocSize;
    }

    /**
     * method to allow setting preallocate size
     * of log file to pad the file.
     *
     * @param size the size to set to in bytes
     */
    public static void setPreallocSize(long size) {
        preAllocSize = size;
    }

    public void setCurrentSize(long currentSize) {
        this.currentSize = currentSize;
    }

    /**
     * pad the current file to increase its size to the next multiple of preAllocSize greater than the current size and position
     *
     * @param fileChannel the fileChannel of the file to be padded 要填充的事物日志文件对应的FileChannel
     * @throws IOException
     */
    // 填充内存
    long padFile(FileChannel fileChannel) throws IOException {
        // 重新计算fileChannel需要与分配的空间
        // fileChannel.position()获取已写入的字节数
        long newFileSize = calculateFileSizeWithPadding(fileChannel.position(), currentSize, preAllocSize);
        // 旧的预分配的文件字节数 != 新的预分配的文件字节数
        // 填充0
        if (currentSize != newFileSize) {
            // 新文件填充0
            fileChannel.write((ByteBuffer) fill.position(0), newFileSize - fill.remaining());
            currentSize = newFileSize;
        }
        // 预分配的文件大小(有可能是之前旧值也有可能是新值)
        return currentSize;
    }

    /**
     * Calculates a new file size with padding. We only return a new size if
     * the current file position is sufficiently close (less than 4K) to end of
     * file and preAllocSize is > 0.
     * 重新计算预分配文件的大小
     * @param position     the point in the file we have written to 当前文件已写入的字节数
     * @param fileSize     application keeps track of the current file size 当前文件预分配的字节数
     * @param preAllocSize how many bytes to pad
     * @return the new file size. It can be the same as fileSize if no
     * padding was done.
     * @throws IOException
     */
    // VisibleForTesting
    public static long calculateFileSizeWithPadding(long position, long fileSize, long preAllocSize) {
        // If preAllocSize is positive and we are within 4KB of the known end of the file calculate a new file size
        // 当前文件已写入的字节数 + 4k >= 预分配的大小了,那么需要重新计算预分配的大小
        if (preAllocSize > 0 && position + 4096 >= fileSize) {
            // If we have written more than we have previously preallocated we need to make sure the new
            // file size is larger than what we already have
            // 如果写入的字节数已经 > 之前预分配的字节数了
            if (position > fileSize) {
                // 重新计算一个接近preAllocSize整数倍的值
                // 该值一定大于当前已文件已写入的字节数
                fileSize = position + preAllocSize;
                fileSize -= fileSize % preAllocSize;
            } else {
                // 如果写入的字节数 <= 之前预分配的字节数
                // 在之前预分配的字节数基础上加上一个preAllocSize
                fileSize += preAllocSize;
            }
        }
        return fileSize;
    }
}
