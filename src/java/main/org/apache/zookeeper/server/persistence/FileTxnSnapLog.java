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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper class 
 * above the implementations 
 * of txnlog and snapshot 
 * classes
 */
// 事物日志和数据快照类
public class FileTxnSnapLog {
    //the direcotry containing the 
    //the transaction logs
    // 事物日志文件
    private final File dataDir;
    //the directory containing the
    //the snapshot directory
    // 数据快照文件
    private final File snapDir;
    // 事物日志类
    private TxnLog txnLog;
    // 数据快照类
    private SnapShot snapLog;
    // 以下两个属性拼接了日志文件的目录(也就是version-2)
    // 参考构造方法
    public final static int VERSION = 2;
    public final static String version = "version-";
    
    private static final Logger LOG = LoggerFactory.getLogger(FileTxnSnapLog.class);
    
    /**
     * This listener helps
     * the external apis calling
     * restore to gather information
     * while the data is being 
     * restored.
     */
    public interface PlayBackListener {
        void onTxnLoaded(TxnHeader hdr, Record rec);
    }
    
    /**
     * the constructor which takes the datadir and 
     * snapdir.
     * @param dataDir the trasaction directory 事务日志文件夹
     * @param snapDir the snapshot directory 数据快照文件夹
     */
    public FileTxnSnapLog(File dataDir, File snapDir) throws IOException {
        LOG.debug("Opening datadir:{} snapDir:{}", dataDir, snapDir);
        // 创建数据快照和事物日志对应的文件
        this.dataDir = new File(dataDir, version + VERSION);
        this.snapDir = new File(snapDir, version + VERSION);
        // 做一些校验
        if (!this.dataDir.exists()) {
            if (!this.dataDir.mkdirs()) {
                throw new IOException("Unable to create data directory "
                        + this.dataDir);
            }
        }
        if (!this.dataDir.canWrite()) {
            throw new IOException("Cannot write to data directory " + this.dataDir);
        }

        if (!this.snapDir.exists()) {
            if (!this.snapDir.mkdirs()) {
                throw new IOException("Unable to create snap directory "
                        + this.snapDir);
            }
        }
        if (!this.snapDir.canWrite()) {
            throw new IOException("Cannot write to snap directory " + this.snapDir);
        }

        // check content of transaction log and snapshot dirs if they are two different directories
        // See ZOOKEEPER-2967 for more details
        // 如果数据快照和事物日志路径不一样，则分别进行不同的校验
        if(!this.dataDir.getPath().equals(this.snapDir.getPath())){
            checkLogDir();
            checkSnapDir();
        }
        // 分别创建事物日志和数据快照两个类并传入对应的日志目录
        txnLog = new FileTxnLog(this.dataDir);
        snapLog = new FileSnap(this.snapDir);
    }

    public void setServerStats(ServerStats serverStats) {
        txnLog.setServerStats(serverStats);
    }

    /**
     * 校验的前提是事物日志和数据快照不在同一个目录
     * 校验事物日志文件夹
     * @throws LogDirContentCheckException
     */
    private void checkLogDir() throws LogDirContentCheckException {
        // 获取事物日志文件夹目录下的文件
        File[] files = this.dataDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return Util.isSnapshotFileName(name);
            }
        });
        if (files != null && files.length > 0) {
            throw new LogDirContentCheckException("Log directory has snapshot files. Check if dataLogDir and dataDir configuration is correct.");
        }
    }

    /**
     *
     * 校验的前提是事物日志和数据快照不在同一个目录
     * 校验数据快照文件夹
     *
     * @throws SnapDirContentCheckException
     */
    private void checkSnapDir() throws SnapDirContentCheckException {
        File[] files = this.snapDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return Util.isLogFileName(name);
            }
        });
        if (files != null && files.length > 0) {
            throw new SnapDirContentCheckException("Snapshot directory has log files. Check if dataLogDir and dataDir configuration is correct.");
        }
    }

    /**
     * get the datadir used by this filetxn
     * snap log
     * @return the data dir
     */
    public File getDataDir() {
        return this.dataDir;
    }
    
    /**
     * get the snap dir used by this 
     * filetxn snap log
     * @return the snap dir
     */
    public File getSnapDir() {
        return this.snapDir;
    }
    
    /**
     * this function restores the server 
     * database after reading from the 
     * snapshots and transaction logs
     * @param dt the datatree to be restored
     * @param sessions the sessions to be restored
     * @param listener the playback listener to run on the 
     * database restoration
     * @return the highest zxid restored
     * @throws IOException
     */
    // 恢复当前服务器上存储的数据快照和事物日志
    public long restore(DataTree dt, Map<Long, Integer> sessions, 
            PlayBackListener listener) throws IOException {
        snapLog.deserialize(dt, sessions);
        return fastForwardFromEdits(dt, sessions, listener);
    }

    /**
     * This function will fast forward the server database to have the latest
     * transactions in it.  This is the same as restore, but only reads from
     * the transaction logs and not restores from a snapshot.
     * @param dt the datatree to write transactions to.
     * @param sessions the sessions to be restored.
     * @param listener the playback listener to run on the
     * database transactions.
     * @return the highest zxid restored.
     * @throws IOException
     */
    public long fastForwardFromEdits(DataTree dt, Map<Long, Integer> sessions,
                                     PlayBackListener listener) throws IOException {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        // 读取磁盘上大于lastProcessedZxid+1的事务日志
        TxnIterator itr = txnLog.read(dt.lastProcessedZxid+1);
        long highestZxid = dt.lastProcessedZxid;
        TxnHeader hdr;
        try {
            while (true) {
                // iterator points to 
                // the first valid txn when initialized
                hdr = itr.getHeader();
                if (hdr == null) {
                    //empty logs 
                    return dt.lastProcessedZxid;
                }
                if (hdr.getZxid() < highestZxid && highestZxid != 0) {
                    LOG.error("{}(higestZxid) > {}(next log) for type {}",
                            new Object[] { highestZxid, hdr.getZxid(),
                                    hdr.getType() });
                } else {
                    highestZxid = hdr.getZxid();
                }
                try {
                    // 回放磁盘事务日志到内存
                    processTransaction(hdr,dt,sessions, itr.getTxn());
                } catch(KeeperException.NoNodeException e) {
                   throw new IOException("Failed to process transaction type: " +
                         hdr.getType() + " error: " + e.getMessage(), e);
                }
                // 磁盘数据回放后触发监听器的执行
                listener.onTxnLoaded(hdr, itr.getTxn());
                if (!itr.next()) 
                    break;
            }
        } finally {
            if (itr != null) {
                itr.close();
            }
        }
        return highestZxid;
    }
    
    /**
     * process the transaction on the datatree
     * @param hdr the hdr of the transaction
     * @param dt the datatree to apply transaction to
     * @param sessions the sessions to be restored
     * @param txn the transaction to be applied
     */
    // 处理事务请求
    public void processTransaction(TxnHeader hdr,DataTree dt,
            Map<Long, Integer> sessions, Record txn)
        throws KeeperException.NoNodeException {
        ProcessTxnResult rc;
        switch (hdr.getType()) {
        case OpCode.createSession:
            sessions.put(hdr.getClientId(),
                    ((CreateSessionTxn) txn).getTimeOut());
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG,ZooTrace.SESSION_TRACE_MASK,
                        "playLog --- create session in log: 0x"
                                + Long.toHexString(hdr.getClientId())
                                + " with timeout: "
                                + ((CreateSessionTxn) txn).getTimeOut());
            }
            // give dataTree a chance to sync its lastProcessedZxid
            rc = dt.processTxn(hdr, txn);
            break;
        case OpCode.closeSession:
            sessions.remove(hdr.getClientId());
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG,ZooTrace.SESSION_TRACE_MASK,
                        "playLog --- close session in log: 0x"
                                + Long.toHexString(hdr.getClientId()));
            }
            rc = dt.processTxn(hdr, txn);
            break;
        default:
            rc = dt.processTxn(hdr, txn);
        }

        /**
         * Snapshots are lazily created. So when a snapshot is in progress,
         * there is a chance for later transactions to make into the
         * snapshot. Then when the snapshot is restored, NONODE/NODEEXISTS
         * errors could occur. It should be safe to ignore these.
         */
        if (rc.err != Code.OK.intValue()) {
            LOG.debug("Ignoring processTxn failure hdr:" + hdr.getType()
                    + ", error: " + rc.err + ", path: " + rc.path);
        }
    }

    /**
     * the last logged zxid on the transaction logs
     * @return the last logged zxid
     */
    public long getLastLoggedZxid() {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        return txnLog.getLastLoggedZxid();
    }

    /**
     * save the datatree and the sessions into a snapshot
     * @param dataTree the datatree to be serialized onto disk
     * @param sessionsWithTimeouts the sesssion timeouts to be
     * serialized onto disk
     * @throws IOException
     */
    // 将内存目录树和会话对应的超时时间记录到数据快照文件
    public void save(DataTree dataTree,
            ConcurrentHashMap<Long, Integer> sessionsWithTimeouts)
        throws IOException {
        // 获取最后处理的zxid
        long lastZxid = dataTree.lastProcessedZxid;
        // 基于最后的zxid创建一个新的数据快照文件
        File snapshotFile = new File(snapDir, Util.makeSnapshotName(lastZxid));
        LOG.info("Snapshotting: 0x{} to {}", Long.toHexString(lastZxid),
                snapshotFile);
        // 序列化到数据快照文件
        snapLog.serialize(dataTree, sessionsWithTimeouts, snapshotFile);
        
    }

    /**
     * truncate the transaction logs the zxid
     * specified
     * @param zxid the zxid to truncate the logs to
     * @return true if able to truncate the log, false if not
     * @throws IOException
     */
    public boolean truncateLog(long zxid) throws IOException {
        // close the existing txnLog and snapLog
        close();

        // truncate it
        FileTxnLog truncLog = new FileTxnLog(dataDir);
        boolean truncated = truncLog.truncate(zxid);
        truncLog.close();

        // re-open the txnLog and snapLog
        // I'd rather just close/reopen this object itself, however that 
        // would have a big impact outside ZKDatabase as there are other
        // objects holding a reference to this object.
        txnLog = new FileTxnLog(dataDir);
        snapLog = new FileSnap(snapDir);

        return truncated;
    }
    
    /**
     * the most recent snapshot in the snapshot
     * directory
     * @return the file that contains the most 
     * recent snapshot
     * @throws IOException
     */
    public File findMostRecentSnapshot() throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findMostRecentSnapshot();
    }
    
    /**
     * the n most recent snapshots
     * @param n the number of recent snapshots
     * @return the list of n most recent snapshots, with
     * the most recent in front
     * @throws IOException
     */
    // 最近的n个数据快照文件
    public List<File> findNRecentSnapshots(int n) throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findNRecentSnapshots(n);
    }

    /**
     * get the snapshot logs which may contain transactions newer than the given zxid.
     * This includes logs with starting zxid greater than given zxid, as well as the
     * newest transaction log with starting zxid less than given zxid.  The latter log
     * file may contain transactions beyond given zxid.
     * @param zxid the zxid that contains logs greater than
     * zxid
     * @return
     */
    // 获取事物日志文件中 >= zxid的文件
    // 这些文件也就是需要保留下来的事务日志文件
    public File[] getSnapshotLogs(long zxid) {
        return FileTxnLog.getLogFiles(dataDir.listFiles(), zxid);
    }

    /**
     * append the request to the transaction logs
     * @param si the request to be appended
     * returns true iff something appended, otw false 
     * @throws IOException
     */
    public boolean append(Request si) throws IOException {
        return txnLog.append(si.hdr, si.txn);
    }

    /**
     * 提交日志事务
     * commit the transaction of logs
     * @throws IOException
     */
    public void commit() throws IOException {
        txnLog.commit();
    }

    /**
     * roll the transaction logs
     * @throws IOException 
     */
    public void rollLog() throws IOException {
        txnLog.rollLog();
    }
    
    /**
     * close the transaction log files
     * @throws IOException
     */
    public void close() throws IOException {
        txnLog.close();
        snapLog.close();
    }

    @SuppressWarnings("serial")
    public static class DatadirException extends IOException {
        public DatadirException(String msg) {
            super(msg);
        }
        public DatadirException(String msg, Exception e) {
            super(msg, e);
        }
    }

    @SuppressWarnings("serial")
    public static class LogDirContentCheckException extends DatadirException {
        public LogDirContentCheckException(String msg) {
            super(msg);
        }
    }

    @SuppressWarnings("serial")
    public static class SnapDirContentCheckException extends DatadirException {
        public SnapDirContentCheckException(String msg) {
            super(msg);
        }
    }
}
