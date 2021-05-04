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
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the TxnLog interface. It provides api's
 * to access the txnlogs and add entries to it.
 * <p>
 * The format of a Transactional log is as follows:
 * <blockquote><pre>
 * LogFile:
 *     FileHeader TxnList ZeroPad
 * 
 * FileHeader: {
 *     magic 4bytes (ZKLG)
 *     version 4bytes
 *     dbid 8bytes
 *   }
 * 
 * TxnList:
 *     Txn || Txn TxnList
 *     
 * Txn:
 *     checksum Txnlen TxnHeader Record 0x42
 * 
 * checksum: 8bytes Adler32 is currently used
 *   calculated across payload -- Txnlen, TxnHeader, Record and 0x42
 * 
 * Txnlen:
 *     len 4bytes
 * 
 * TxnHeader: {
 *     sessionid 8bytes
 *     cxid 4bytes
 *     zxid 8bytes
 *     time 8bytes
 *     type 4bytes
 *   }
 *     
 * Record:
 *     See Jute definition file for details on the various record types
 *      
 * ZeroPad:
 *     0 padded to EOF (filled during preallocation stage)
 * </pre></blockquote> 
 */
// 事物日志类，提供了访问和添加事物日志的功能
public class FileTxnLog implements TxnLog {
    private static final Logger LOG;
    // 魔术
    public final static int TXNLOG_MAGIC =
        ByteBuffer.wrap("ZKLG".getBytes()).getInt();

    public final static int VERSION = 2;
    // 事物日志文件前缀
    public static final String LOG_FILE_PREFIX = "log";

    /** Maximum time we allow for elapsed fsync before WARNing */
    // 刷磁盘时的最大等待阈值,超过这个值会报警
    private final static long fsyncWarningThresholdMS;

    static {
        LOG = LoggerFactory.getLogger(FileTxnLog.class);

        /** Local variable to read fsync.warningthresholdms into */
        Long fsyncWarningThreshold;
        if ((fsyncWarningThreshold = Long.getLong("zookeeper.fsync.warningthresholdms")) == null)
            fsyncWarningThreshold = Long.getLong("fsync.warningthresholdms", 1000);
        fsyncWarningThresholdMS = fsyncWarningThreshold;
    }
    // 记录最后写入事务文件的txid
    long lastZxidSeen;
    // 记录当前活跃事务日志文件的流
    volatile BufferedOutputStream logStream = null;
    // 记录当前活跃事务日志文件的流
    volatile OutputArchive oa;
    // 记录当前活跃事务日志文件的流
    volatile FileOutputStream fos = null;
    // 事务日志文件对应的文件目录
    File logDir;
    private final boolean forceSync = !System.getProperty("zookeeper.forceSync", "yes").equals("no");;
    long dbId;
    // 待同步刷磁盘的文件流
    private LinkedList<FileOutputStream> streamsToFlush =
        new LinkedList<FileOutputStream>();
    // 当前事务日志文件
    File logFileWrite = null;
    // 用来预分配当前的事务日志文件
    // 首先里面记录了当前事务日志文件写入的子节点
    private FilePadding filePadding = new FilePadding();
    // zk服务统计类,来自ZookeeperServer
    private ServerStats serverStats;

    /**
     * constructor for FileTxnLog. Take the directory
     * where the txnlogs are stored
     * @param logDir the directory where the txnlogs are stored
     */
    public FileTxnLog(File logDir) {
        this.logDir = logDir;
    }

    /**
      * method to allow setting preallocate size
      * of log file to pad the file.
      * @param size the size to set to in bytes
      */
    // 设置预分配大小,默认64M
    public static void setPreallocSize(long size) {
        FilePadding.setPreallocSize(size);
    }

    /**
     * Setter for ServerStats to monitor fsync threshold exceed
     * @param serverStats used to update fsyncThresholdExceedCount
     */
    // 设置服务状态
     @Override
     public void setServerStats(ServerStats serverStats) {
         this.serverStats = serverStats;
     }

    /**
     * creates a checksum algorithm to be used
     * @return the checksum used for this txnlog
     */
    // 创建校验算法
    protected Checksum makeChecksumAlgorithm(){
        return new Adler32();
    }

    /**
     * rollover the current log file to a new one.
     * @throws IOException
     */
    // 将当前事务日志文件对应的流刷入磁盘
    // 然后重置流,也就是等待后续重新创建一个日志文件
    // 这里并没有处理fos,因为fos记录到了streamsToFlush集合中
    public synchronized void rollLog() throws IOException {
        if (logStream != null) {
            this.logStream.flush();
            this.logStream = null;
            oa = null;
        }
    }

    /**
     * close all the open file handles
     * @throws IOException
     */
    // 关闭所有文件
    public synchronized void close() throws IOException {
        if (logStream != null) {
            logStream.close();
        }
        for (FileOutputStream log : streamsToFlush) {
            log.close();
        }
    }
    
    /**
     * 添加事物请求到事务日志
     * hdr的创建都是统一的:new TxnHeader(request.sessionId, request.cxid, zxid,Time.currentWallTime(), type)
     * txn根据不同的请求创建的不一样
     * append an entry to the transaction log
     * @param hdr the header of the transaction
     * @param txn the transaction part of the entry
     * returns true iff something appended, otw false 
     */
    public synchronized boolean append(TxnHeader hdr, Record txn)
        throws IOException
    {
        // 校验事务头为空不处理
        if (hdr == null) {
            return false;
        }
        // 更新lastZxidSeen为最大值,因为zxid是递增的
        if (hdr.getZxid() <= lastZxidSeen) {
            LOG.warn("Current zxid " + hdr.getZxid()
                    + " is <= " + lastZxidSeen + " for "
                    + hdr.getType());
        } else {
            lastZxidSeen = hdr.getZxid();
        }
        // 事务日志流未初始化
        if (logStream==null) {
            // 执行到这里有两种可能 1.logStream未初始化 2.logStream被rollLog()方法置为null
           if(LOG.isInfoEnabled()){
                LOG.info("Creating new log file: " + Util.makeLogName(hdr.getZxid()));
           }
           // 根据请求中的zxid创建事物日志文件
           logFileWrite = new File(logDir, Util.makeLogName(hdr.getZxid()));
           // 根据新创建的事物日志文件初始化相关流
           // FileOutputStream无缓冲区,write()一次直接写入文件,无flush()
           fos = new FileOutputStream(logFileWrite);
           // BufferedOutputStream有缓存区,write()一次写入缓冲区,有flush()
           logStream=new BufferedOutputStream(fos);
           // BinaryOutputArchive zk自定义的
           oa = BinaryOutputArchive.getArchive(logStream);
           // 创建并写入事务日志文件头,包括:魔数,版本,数据库ID(默认是0)
            // 也就如类注释中锁描述的那样
            // FileHeader: {
            //  magic 4bytes (ZKLG)
            //  version 4bytes
            //  dbid 8bytes
            // }
           FileHeader fhdr = new FileHeader(TXNLOG_MAGIC,VERSION, dbId);
           fhdr.serialize(oa, "fileheader");
           // Make sure that the magic number is written before padding.
           // 确保在填充文件前，将事务日志文件头写入到文件中
           logStream.flush();
           // 将当前事务日志文件已写入的字节数设置到filePadding的currentSize属性中
            // 表示当前事务日志文件预分配的大小
           filePadding.setCurrentSize(fos.getChannel().position());
           // 将新生成的事务日志文件记录到streamsToFlush中表示待输入磁盘
           // streamsToFlush是一个集合,里面可能记录了多个fos
           streamsToFlush.add(fos);
        }
        // 获取当前事务日志文件的FileChannel传入filePadding中
        // 目的是计算FileChannel是否需要预分配
        filePadding.padFile(fos.getChannel());
        // 序列化hdr和txn为byte数组
        // checksum Txnlen TxnHeader Record 0x42
        byte[] buf = Util.marshallTxnEntry(hdr, txn);
        if (buf == null || buf.length == 0) {
            throw new IOException("Faulty serialization for header " +
                    "and txn");
        }
        // 计算CRC
        Checksum crc = makeChecksumAlgorithm();
        crc.update(buf, 0, buf.length);
        // 写入CRC和上面的byte数组
        oa.writeLong(crc.getValue(), "txnEntryCRC");
        Util.writeTxnBytes(oa, buf);

        return true;
    }

    /**
     * Find the log file that starts at, or just before, the snapshot. Return
     * this and all subsequent logs. Results are ordered by zxid of file,
     * ascending order.
     * @param logDirList array of files
     * @param snapshotZxid return files at, or before this zxid
     * @return
     */
    // 从参数logDirList中获取snapshotZxid所在事务日志文件及之后的所有事务文件
    // 这些事务日志文件是需要保留下来的
    public static File[] getLogFiles(File[] logDirList,long snapshotZxid) {
        // 获取所有的事务日志文件,按照文件中最小的zxid升序排列
        List<File> files = Util.sortDataDir(logDirList, LOG_FILE_PREFIX, true);
        long logZxid = 0;
        // Find the log file that starts before or at the same time as the
        // zxid of the snapshot
        // 查找小于snapshotZxid并且最接近它的那个事物文件
        // 注:事务文件名中包含了当前事务文件中最小的zxid
        for (File f : files) {
            // 获取事务日志文件名中的zxid,也就是当前文件中记录的最小的zxid
            long fzxid = Util.getZxidFromName(f.getName(), LOG_FILE_PREFIX);
            // 当前事务日志文件中最小的zxid还大于参数snapshotZxid,不处理这个文件
            if (fzxid > snapshotZxid) {
                continue;
            }
            // the files
            // are sorted with zxid's
            // 记录小于并且最接近snapshotZxid的事务日志文件的zxid
            if (fzxid > logZxid) {
                logZxid = fzxid;
            }
        }
        // 查找logZxid之后的事物文件
        List<File> v=new ArrayList<File>(5);
        for (File f : files) {
            long fzxid = Util.getZxidFromName(f.getName(), LOG_FILE_PREFIX);
            if (fzxid < logZxid) {
                continue;
            }
            v.add(f);
        }
        return v.toArray(new File[0]);

    }

    /**
     * get the last zxid that was logged in the transaction logs
     * @return the last zxid logged in the transaction logs
     */
    // 获取事务日志中记录的最后一个zxid
    public long getLastLoggedZxid() {
        // 获取事务日志文件目录下的所有事物日志文件
        File[] files = getLogFiles(logDir.listFiles(), 0);
        // 如果存在事务文件则从最后一个文件中解析出zxid,否则zxid为-1
        long maxLog=files.length>0?
                Util.getZxidFromName(files[files.length-1].getName(),LOG_FILE_PREFIX):-1;

        // if a log file is more recent we must scan it to find
        // the highest zxid
        long zxid = maxLog;
        TxnIterator itr = null;
        try {
            FileTxnLog txn = new FileTxnLog(logDir);
            itr = txn.read(maxLog);
            while (true) {
                if(!itr.next())
                    break;
                TxnHeader hdr = itr.getHeader();
                zxid = hdr.getZxid();
            }
        } catch (IOException e) {
            LOG.warn("Unexpected exception", e);
        } finally {
            close(itr);
        }
        return zxid;
    }

    private void close(TxnIterator itr) {
        if (itr != null) {
            try {
                itr.close();
            } catch (IOException ioe) {
                LOG.warn("Error closing file iterator", ioe);
            }
        }
    }

    /**
     * commit the logs. make sure that evertyhing hits the
     * disk
     */
    // 提交事务日志
    public synchronized void commit() throws IOException {
        // logStream不为空时,logStream刷磁盘
        if (logStream != null) {
            logStream.flush();
        }
        // 遍历streamsToFlush,也刷磁盘
        for (FileOutputStream log : streamsToFlush) {
            // FileOutputStream并没有实现flush()方法,而父类flush()什么也没做,所以这里操作意义是什么?
            log.flush();
            // 如果强制刷磁盘,则判断是否超时,超时报警
            if (forceSync) {
                long startSyncNS = System.nanoTime();

                log.getChannel().force(false);
                // 计算刷磁盘的时间
                long syncElapsedMS =
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startSyncNS);
                // 超时报警
                if (syncElapsedMS > fsyncWarningThresholdMS) {
                    if(serverStats != null) {
                        serverStats.incrementFsyncThresholdExceedCount();
                    }
                    LOG.warn("fsync-ing the write ahead log in "
                            + Thread.currentThread().getName()
                            + " took " + syncElapsedMS
                            + "ms which will adversely effect operation latency. "
                            + "See the ZooKeeper troubleshooting guide");
                }
            }
        }
        while (streamsToFlush.size() > 1) {
            streamsToFlush.removeFirst().close();
        }
    }

    /**
     * start reading all the transactions from the given zxid
     * @param zxid the zxid to start reading transactions from
     * @return returns an iterator to iterate through the transaction
     * logs
     */
    public TxnIterator read(long zxid) throws IOException {
        return new FileTxnIterator(logDir, zxid);
    }

    /**
     * truncate the current transaction logs
     * @param zxid the zxid to truncate the logs to
     * @return true if successful false if not
     */
    // 截断当前事务日志到指定的zxid
    public boolean truncate(long zxid) throws IOException {
        FileTxnIterator itr = null;
        try {
            itr = new FileTxnIterator(this.logDir, zxid);
            PositionInputStream input = itr.inputStream;
            if(input == null) {
                throw new IOException("No log files found to truncate! This could " +
                        "happen if you still have snapshots from an old setup or " +
                        "log files were deleted accidentally or dataLogDir was changed in zoo.cfg.");
            }
            // 获取当前文件已输入的位置,然后截断到该位置
            long pos = input.getPosition();
            // now, truncate at the current position
            RandomAccessFile raf = new RandomAccessFile(itr.logFile, "rw");
            raf.setLength(pos);
            raf.close();
            // 递归删除文件
            while (itr.goToNextLog()) {
                if (!itr.logFile.delete()) {
                    LOG.warn("Unable to truncate {}", itr.logFile);
                }
            }
        } finally {
            close(itr);
        }
        return true;
    }

    /**
     * read the header of the transaction file
     * @param file the transaction file to read
     * @return header that was read fomr the file
     * @throws IOException
     */
    // 读取事务日志文件头
    private static FileHeader readHeader(File file) throws IOException {
        InputStream is =null;
        try {
            is = new BufferedInputStream(new FileInputStream(file));
            InputArchive ia=BinaryInputArchive.getArchive(is);
            FileHeader hdr = new FileHeader();
            hdr.deserialize(ia, "fileheader");
            return hdr;
         } finally {
             try {
                 if (is != null) is.close();
             } catch (IOException e) {
                 LOG.warn("Ignoring exception during close", e);
             }
         }
    }

    /**
     * the dbid of this transaction database
     * @return the dbid of this database
     */
    // 获取事务日志的db
    public long getDbId() throws IOException {
        FileTxnIterator itr = new FileTxnIterator(logDir, 0);
        FileHeader fh=readHeader(itr.logFile);
        itr.close();
        if(fh==null)
            throw new IOException("Unsupported Format.");
        return fh.getDbid();
    }

    /**
     * the forceSync value. true if forceSync is enabled, false otherwise.
     * @return the forceSync value
     */
    // 是否强制刷磁盘
    public boolean isForceSync() {
        return forceSync;
    }

    /**
     * a class that keeps track of the position 
     * in the input stream. The position points to offset
     * that has been consumed by the applications. It can 
     * wrap buffered input streams to provide the right offset 
     * for the application.
     */
    static class PositionInputStream extends FilterInputStream {
        long position;
        protected PositionInputStream(InputStream in) {
            super(in);
            position = 0;
        }
        
        @Override
        public int read() throws IOException {
            int rc = super.read();
            if (rc > -1) {
                position++;
            }
            return rc;
        }

        public int read(byte[] b) throws IOException {
            int rc = super.read(b);
            if (rc > 0) {
                position += rc;
            }
            return rc;            
        }
        
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int rc = super.read(b, off, len);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }
        
        @Override
        public long skip(long n) throws IOException {
            long rc = super.skip(n);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }
        public long getPosition() {
            return position;
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void mark(int readLimit) {
            throw new UnsupportedOperationException("mark");
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException("reset");
        }
    }
    
    /**
     * this class implements the txnlog iterator interface
     * which is used for reading the transaction logs
     */
    public static class FileTxnIterator implements TxnLog.TxnIterator {
        // 日志文件目录
        File logDir;
        // 指定的zxid
        long zxid;
        TxnHeader hdr;
        Record record;
        // 当前待处理日志文件
        File logFile;
        // 当前待处理日志文件的InputArchive
        InputArchive ia;
        static final String CRC_ERROR="CRC check failed";
       
        PositionInputStream inputStream=null;
        //stored files is the list of files greater than
        //the zxid we are looking for.
        // 存储大于正在寻找的zxid的文件列表
        private ArrayList<File> storedFiles;

        /**
         * create an iterator over a transaction database directory
         * @param logDir the transaction database directory
         * @param zxid the zxid to start reading from
         * @throws IOException
         */
        public FileTxnIterator(File logDir, long zxid) throws IOException {
          this.logDir = logDir;
          this.zxid = zxid;
          init();
        }

        /**
         * initialize to the zxid specified
         * this is inclusive of the zxid
         * @throws IOException
         */
        // 初始化
        void init() throws IOException {
            storedFiles = new ArrayList<File>();
            // 1.logDir.listFiles() 获取logDir目录下的所以日志文件(日志文件名包含当前日志记录的最小zxid)
            // 2.FileTxnLog.getLogFiles(logDir.listFiles(), 0)获取zxid >= 0 的所有日志文件
            // 3.之后过滤出所有的.log结尾的文件并降序排列
            List<File> files = Util.sortDataDir(FileTxnLog.getLogFiles(logDir.listFiles(), 0), LOG_FILE_PREFIX, false);
            // 遍历过滤出来的所有文件
            for (File f: files) {
                // 如果文件的最小zxid >= 指定的zxid,记录下来该日志文件
                if (Util.getZxidFromName(f.getName(), LOG_FILE_PREFIX) >= zxid) {
                    storedFiles.add(f);
                }
                // add the last logfile that is less than the zxid
                // 添加最后一个小于zxid的日志文件,由于文件是降序排列,所以当找到第一个小于指定zxid的文件时跳出循环即可
                else if (Util.getZxidFromName(f.getName(), LOG_FILE_PREFIX) < zxid) {
                    storedFiles.add(f);
                    break;
                }
            }
            // 获取下一个日志文件
            goToNextLog();
            // 不存在直接退出当前方法
            // 存在则序列化一条记录
            if (!next())
                return;
            // 初始化 >= zxid的那条记录
            while (hdr.getZxid() < zxid) {
                if (!next())
                    return;
            }
        }

        /**
         * go to the next logfile
         * @return true if there is one and false if there is no
         * new file to be read
         * @throws IOException
         */
        // 转到下一个日志文件,如果有返回true,否则返回false
        private boolean goToNextLog() throws IOException {
            // 有待处理的文件
            if (storedFiles.size() > 0) {
                // 获取一个待处理的文件
                this.logFile = storedFiles.remove(storedFiles.size()-1);
                ia = createInputArchive(this.logFile);
                return true;
            }
            return false;
        }

        /**
         * read the header from the inputarchive
         * @param ia the inputarchive to be read from
         * @param is the inputstream
         * @throws IOException
         */
        // 从inputarchive中读取头文件并校验
        protected void inStreamCreated(InputArchive ia, InputStream is)
            throws IOException{
            FileHeader header= new FileHeader();
            header.deserialize(ia, "fileheader");
            if (header.getMagic() != FileTxnLog.TXNLOG_MAGIC) {
                throw new IOException("Transaction log: " + this.logFile + " has invalid magic number " 
                        + header.getMagic()
                        + " != " + FileTxnLog.TXNLOG_MAGIC);
            }
        }

        /**
         * Invoked to indicate that the input stream has been created.
         * @param ia input archive
         * @param is file input stream associated with the input archive.
         * @throws IOException
         **/
        // 创建InputArchive
        protected InputArchive createInputArchive(File logFile) throws IOException {
            if(inputStream==null){
                inputStream= new PositionInputStream(new BufferedInputStream(new FileInputStream(logFile)));
                LOG.debug("Created new input stream " + logFile);
                ia  = BinaryInputArchive.getArchive(inputStream);
                // 读取文件头并校验
                inStreamCreated(ia,inputStream);
                LOG.debug("Created new input archive " + logFile);
            }
            return ia;
        }

        /**
         * create a checksum algorithm
         * @return the checksum algorithm
         */
        protected Checksum makeChecksumAlgorithm(){
            return new Adler32();
        }

        /**
         * the iterator that moves to the next transaction
         * @return true if there is more transactions to be read
         * false if not.
         */
        // 解析下一个日志文件
        public boolean next() throws IOException {
            if (ia == null) {
                return false;
            }
            try {
                // 获取crc的值
                long crcValue = ia.readLong("crcvalue");
                // 读取数据
                byte[] bytes = Util.readTxnBytes(ia);
                // Since we preallocate, we define EOF to be an
                if (bytes == null || bytes.length==0) {
                    throw new EOFException("Failed to read " + logFile);
                }
                // EOF or corrupted record
                // validate CRC
                // 校验crc
                Checksum crc = makeChecksumAlgorithm();
                crc.update(bytes, 0, bytes.length);
                if (crcValue != crc.getValue())
                    throw new IOException(CRC_ERROR);
                // 解析数据中的请求头然后序列化数据记录到record
                hdr = new TxnHeader();
                record = SerializeUtils.deserializeTxn(bytes, hdr);
            } catch (EOFException e) {
                LOG.debug("EOF excepton " + e);
                inputStream.close();
                inputStream = null;
                ia = null;
                hdr = null;
                // this means that the file has ended
                // we should go to the next file
                if (!goToNextLog()) {
                    return false;
                }
                // if we went to the next log file, we should call next() again
                return next();
            } catch (IOException e) {
                inputStream.close();
                throw e;
            }
            return true;
        }

        /**
         * reutrn the current header
         * @return the current header that
         * is read
         */
        public TxnHeader getHeader() {
            return hdr;
        }

        /**
         * return the current transaction
         * @return the current transaction
         * that is read
         */
        public Record getTxn() {
            return record;
        }

        /**
         * close the iterator
         * and release the resources.
         */
        public void close() throws IOException {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

}
