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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.util.SerializeUtils;

/**
 * This class implements the snapshot interface.
 * it is responsible for storing, serializing
 * and deserializing the right snapshot.
 * and provides access to the snapshots.
 */
// 实现SnapShot接口，实现了存储、序列化、反序列化数据快照功能
// 同时提供了访问数据快照的功能
public class FileSnap implements SnapShot {
    // 快照文件对应的文件夹目录
    File snapDir;
    private volatile boolean close = false;
    private static final int VERSION=2;
    private static final long dbId=-1;
    private static final Logger LOG = LoggerFactory.getLogger(FileSnap.class);
    // 数据快照日志文件魔术
    public final static int SNAP_MAGIC
        = ByteBuffer.wrap("ZKSN".getBytes()).getInt();
    // 快照文件前缀
    public static final String SNAPSHOT_FILE_PREFIX = "snapshot";

    public FileSnap(File snapDir) {
        this.snapDir = snapDir;
    }

    /**
     * deserialize a data tree from the most recent snapshot
     * @return the zxid of the snapshot
     */
    // 反序列化有效的数据快照文件到内存文件目录树DataTree
    public long deserialize(DataTree dt, Map<Long, Integer> sessions)
            throws IOException {
        // we run through 100 snapshots (not all of them)
        // if we cannot get it running within 100 snapshots
        // we should  give up
        // 查找最近的100个有效的数据快照日志文件并降序排列
        List<File> snapList = findNValidSnapshots(100);
        if (snapList.size() == 0) {
            return -1L;
        }
        // 标识数据快照日志文件
        File snap = null;
        // 标识数据快照日志文件是否有效
        boolean foundValid = false;
        for (int i = 0; i < snapList.size(); i++) {
            snap = snapList.get(i);
            InputStream snapIS = null;
            CheckedInputStream crcIn = null;
            try {
                LOG.info("Reading snapshot " + snap);
                snapIS = new BufferedInputStream(new FileInputStream(snap));
                crcIn = new CheckedInputStream(snapIS, new Adler32());
                InputArchive ia = BinaryInputArchive.getArchive(crcIn);
                // 反序列化
                deserialize(dt,sessions, ia);
                // 计算数据快照日志文件CRC
                long checkSum = crcIn.getChecksum().getValue();
                // 获取数据快照日志文件CRC
                long val = ia.readLong("val");
                // 校验是否一致
                if (val != checkSum) {
                    throw new IOException("CRC corruption in snapshot :  " + snap);
                }
                // 查找到一个有效的数据快照日志文件,跳出循环
                foundValid = true;
                break;
            } catch(IOException e) {
                LOG.warn("problem reading snap file " + snap, e);
            } finally {
                if (snapIS != null) 
                    snapIS.close();
                if (crcIn != null) 
                    crcIn.close();
            } 
        }
        if (!foundValid) {
            throw new IOException("Not able to find valid snapshots in " + snapDir);
        }
        // 获取zxid并返回
        dt.lastProcessedZxid = Util.getZxidFromName(snap.getName(), SNAPSHOT_FILE_PREFIX);
        return dt.lastProcessedZxid;
    }

    /**
     * deserialize the datatree from an inputarchive
     * @param dt the datatree to be serialized into
     * @param sessions the sessions to be filled up
     * @param ia the input archive to restore from
     * @throws IOException
     */
    // 反序列化
    public void deserialize(DataTree dt, Map<Long, Integer> sessions,
            InputArchive ia) throws IOException {
        FileHeader header = new FileHeader();
        // 获取数据快照日志文件的头
        header.deserialize(ia, "fileheader");
        // 校验魔术
        if (header.getMagic() != SNAP_MAGIC) {
            throw new IOException("mismatching magic headers "
                    + header.getMagic() + 
                    " !=  " + FileSnap.SNAP_MAGIC);
        }
        SerializeUtils.deserializeSnapshot(dt,ia,sessions);
    }

    /**
     * find the most recent snapshot in the database.
     * @return the file containing the most recent snapshot
     */
    // 查找最近的一个有效的数据快照日志文件
    public File findMostRecentSnapshot() throws IOException {
        List<File> files = findNValidSnapshots(1);
        if (files.size() == 0) {
            return null;
        }
        return files.get(0);
    }
    
    /**
     * find the last (maybe) valid n snapshots. this does some 
     * minor checks on the validity of the snapshots. It just
     * checks for / at the end of the snapshot. This does
     * not mean that the snapshot is truly valid but is
     * valid with a high probability. also, the most recent 
     * will be first on the list. 
     * @param n the number of most recent snapshots
     * @return the last n snapshots (the number might be
     * less than n in case enough snapshots are not available).
     * @throws IOException
     */
    // 查找最近n个有效的数据快照日志文件
    private List<File> findNValidSnapshots(int n) throws IOException {
        // 获取snapDir目录下所有以snapshot为前缀的文件并降序排列
        List<File> files = Util.sortDataDir(snapDir.listFiles(), SNAPSHOT_FILE_PREFIX, false);
        int count = 0;
        List<File> list = new ArrayList<File>();
        // 遍历数据快照文件
        for (File f : files) {
            // we should catch the exceptions
            // from the valid snapshot and continue
            // until we find a valid one
            try {
                // 校验文件的有效性,如果有效放入list中直到找到参数n个返回list数组
                if (Util.isValidSnapshot(f)) {
                    list.add(f);
                    count++;
                    if (count == n) {
                        break;
                    }
                }
            } catch (IOException e) {
                LOG.info("invalid snapshot " + f, e);
            }
        }
        return list;
    }

    /**
     * find the last n snapshots. this does not have
     * any checks if the snapshot might be valid or not
     * @param the number of most recent snapshots
     * @return the last n snapshots
     * @throws IOException
     */
    // 查找最近n个数据快照日志文件
    // 比如目前磁盘上此次snapshot文件如下：
    // snapshot.0
    // snapshot.1c
    // snapshot.1d
    // snapshot.300000000
    // snapshot.b00000098
    // n为3时，返回的是snapshot.1d、snapshot.300000000、snapshot.b00000098
    public List<File> findNRecentSnapshots(int n) throws IOException {
        // 获取snapDir目录下所有以snapshot为前缀的文件并降序排列
        List<File> files = Util.sortDataDir(snapDir.listFiles(), SNAPSHOT_FILE_PREFIX, false);
        int count = 0;
        // 遍历文件获取n个数据快照文件
        List<File> list = new ArrayList<File>();
        for (File f: files) {
            if (count == n)
                break;
            // 获取文件名的zxid
            if (Util.getZxidFromName(f.getName(), SNAPSHOT_FILE_PREFIX) != -1) {
                count++;
                list.add(f);
            }
        }
        return list;
    }

    /**
     * serialize the datatree and sessions
     * @param dt the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param oa the output archive to serialize into
     * @param header the header of this snapshot
     * @throws IOException
     */
    // 序列化
    protected void serialize(DataTree dt,Map<Long, Integer> sessions,
            OutputArchive oa, FileHeader header) throws IOException {
        // this is really a programmatic error and not something that can
        // happen at runtime
        if(header==null)
            throw new IllegalStateException(
                    "Snapshot's not open for writing: uninitialized header");
        // 序列化数据文件的头
        header.serialize(oa, "fileheader");
        // 序列化内存目录树/session集合
        SerializeUtils.serializeSnapshot(dt,oa,sessions);
    }

    /**
     * serialize the datatree and session into the file snapshot
     * @param dt the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param snapShot the file to store snapshot into
     */
    // 序列化
    public synchronized void serialize(DataTree dt, Map<Long, Integer> sessions, File snapShot)
            throws IOException {
        if (!close) {
            // 创建输出流
            OutputStream sessOS = new BufferedOutputStream(new FileOutputStream(snapShot));
            CheckedOutputStream crcOut = new CheckedOutputStream(sessOS, new Adler32());
            //CheckedOutputStream cout = new CheckedOutputStream()
            OutputArchive oa = BinaryOutputArchive.getArchive(crcOut);
            // 创建数据文件的头
            FileHeader header = new FileHeader(SNAP_MAGIC, VERSION, dbId);
            // 序列化数据
            serialize(dt,sessions,oa, header);
            long val = crcOut.getChecksum().getValue();
            oa.writeLong(val, "val");
            oa.writeString("/", "path");
            // 刷磁盘关闭流
            sessOS.flush();
            crcOut.close();
            sessOS.close();
        }
    }

    /**
     * synchronized close just so that if serialize is in place
     * the close operation will block and will wait till serialize
     * is done and will set the close flag
     */
    @Override
    public synchronized void close() throws IOException {
        close = true;
    }

 }
