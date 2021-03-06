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

package org.apache.zookeeper.server;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.sasl.SaslException;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.Environment;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.proto.AuthPacket;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.proto.GetSASLRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetSASLResponse;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.RequestProcessor.RequestProcessorException;
import org.apache.zookeeper.server.ServerCnxn.CloseRequestException;
import org.apache.zookeeper.server.SessionTracker.Session;
import org.apache.zookeeper.server.SessionTracker.SessionExpirer;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.ReadOnlyZooKeeperServer;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements a simple standalone ZooKeeperServer. It sets up the
 * following chain of RequestProcessors to process requests:
 * PrepRequestProcessor -> SyncRequestProcessor -> FinalRequestProcessor
 */
// 所有zk服务器的父类
public class ZooKeeperServer implements SessionExpirer, ServerStats.Provider {
    protected static final Logger LOG;
    
    static {
        LOG = LoggerFactory.getLogger(ZooKeeperServer.class);
        
        Environment.logEnv("Server environment:", LOG);
    }

    protected ZooKeeperServerBean jmxServerBean;
    protected DataTreeBean jmxDataTreeBean;

 
    /**
     * The server delegates loading of the tree to an instance of the interface
     */
    public interface DataTreeBuilder {
        public DataTree build();
    }

    static public class BasicDataTreeBuilder implements DataTreeBuilder {
        public DataTree build() {
            return new DataTree();
        }
    }
    // 默认心跳频率
    public static final int DEFAULT_TICK_TIME = 3000;
    protected int tickTime = DEFAULT_TICK_TIME;
    /** value of -1 indicates unset, use default */
    // 最小的会话Session过期时间
    protected int minSessionTimeout = -1;
    /** value of -1 indicates unset, use default */
    // 最大的会话Session过期时间
    protected int maxSessionTimeout = -1;
    // 会话Session跟踪器
    protected SessionTracker sessionTracker;
    /**
     * FileTxnSnapLog里面记录了数据日志和事物快照的相关类
     * 单机模式初始化在{@link org.apache.zookeeper.server.ZooKeeperServerMain.runFromConfig}
     */
    private FileTxnSnapLog txnLogFactory = null;
    // zk内存数据库
    private ZKDatabase zkDb;
    /**
     * zxid生成器初始值为0
     * zxid分为两部分高32位用来存储每次选举的时代epoch，低32位用来存储事务请求的自增序列
     *可参考{@link ZxidUtils}
     */
    private final AtomicLong hzxid = new AtomicLong(0);
    public final static Exception ok = new Exception("No prob");
    // 首个处理器
    protected RequestProcessor firstProcessor;
    // zk服务器初始状态
    protected volatile State state = State.INITIAL;

    protected enum State {
        INITIAL, RUNNING, SHUTDOWN, ERROR;
    }

    /**
     * This is the secret that we use to generate passwords, for the moment it
     * is more of a sanity check.
     */
    static final private long superSecret = 0XB3415C00L;
    /**
     * 正在处理的客户端连接请求数量
     * 递减操作调用:{@link org.apache.zookeeper.server.FinalRequestProcessor#processRequest(org.apache.zookeeper.server.Request)}
     */
    private final AtomicInteger requestsInProcess = new AtomicInteger(0);
    // 记录更改后还未同步到ZKDatabase(内存数据库)中的节点信息
    final List<ChangeRecord> outstandingChanges = new ArrayList<ChangeRecord>();
    // this data structure must be accessed under the outstandingChanges lock
    // 记录更改后还未同步到ZKDatabase(内存数据库)中的节点信息
    // key是对应的path
    final HashMap<String, ChangeRecord> outstandingChangesForPath =
        new HashMap<String, ChangeRecord>();
    // 连接工厂,用来处理用户客户端的连接
    // 两个实现:NIOServerCnxnFactory(默认)和NettyServerCnxnFactory
    private ServerCnxnFactory serverCnxnFactory;
    // zk服务统计类
    private final ServerStats serverStats;
    // 监听器
    // 单机模式启动时,会创建ZooKeeperServerListenerImpl
    private final ZooKeeperServerListener listener;
    // 单机模式启动时,会注册ZooKeeperServerShutdownHandler
    private ZooKeeperServerShutdownHandler zkShutdownHandler;

    void removeCnxn(ServerCnxn cnxn) {
        zkDb.removeCnxn(cnxn);
    }
 
    /**
     * Creates a ZooKeeperServer instance. Nothing is setup, use the setX
     * methods to prepare the instance (eg datadir, datalogdir, ticktime, 
     * builder, etc...)
     * 
     * @throws IOException
     */
    public ZooKeeperServer() {
        serverStats = new ServerStats(this);
        listener = new ZooKeeperServerListenerImpl(this);
    }
    
    /**
     * Creates a ZooKeeperServer instance. It sets everything up, but doesn't
     * actually start listening for clients until run() is invoked.
     * 
     * @param dataDir the directory to put the data
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory, int tickTime,
            int minSessionTimeout, int maxSessionTimeout,
            DataTreeBuilder treeBuilder, ZKDatabase zkDb) {
        serverStats = new ServerStats(this);
        this.txnLogFactory = txnLogFactory;
        this.txnLogFactory.setServerStats(this.serverStats);
        this.zkDb = zkDb;
        this.tickTime = tickTime;
        this.minSessionTimeout = minSessionTimeout;
        this.maxSessionTimeout = maxSessionTimeout;

        listener = new ZooKeeperServerListenerImpl(this);

        LOG.info("Created server with tickTime " + tickTime
                + " minSessionTimeout " + getMinSessionTimeout()
                + " maxSessionTimeout " + getMaxSessionTimeout()
                + " datadir " + txnLogFactory.getDataDir()
                + " snapdir " + txnLogFactory.getSnapDir());
    }

    /**
     * creates a zookeeperserver instance. 
     * @param txnLogFactory the file transaction snapshot logging class
     * @param tickTime the ticktime for the server
     * @param treeBuilder the datatree builder
     * @throws IOException
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory, int tickTime,
            DataTreeBuilder treeBuilder) throws IOException {
        this(txnLogFactory, tickTime, -1, -1, treeBuilder,
                new ZKDatabase(txnLogFactory));
    }
    
    public ServerStats serverStats() {
        return serverStats;
    }

    public void dumpConf(PrintWriter pwriter) {
        pwriter.print("clientPort=");
        pwriter.println(getClientPort());
        pwriter.print("dataDir=");
        pwriter.println(zkDb.snapLog.getSnapDir().getAbsolutePath());
        pwriter.print("dataLogDir=");
        pwriter.println(zkDb.snapLog.getDataDir().getAbsolutePath());
        pwriter.print("tickTime=");
        pwriter.println(getTickTime());
        pwriter.print("maxClientCnxns=");
        pwriter.println(serverCnxnFactory.getMaxClientCnxnsPerHost());
        pwriter.print("minSessionTimeout=");
        pwriter.println(getMinSessionTimeout());
        pwriter.print("maxSessionTimeout=");
        pwriter.println(getMaxSessionTimeout());

        pwriter.print("serverId=");
        pwriter.println(getServerId());
    }

    /**
     * This constructor is for backward compatibility with the existing unit
     * test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public ZooKeeperServer(File snapDir, File logDir, int tickTime)
            throws IOException {
        this( new FileTxnSnapLog(snapDir, logDir),
                tickTime, new BasicDataTreeBuilder());
    }

    /**
     * Default constructor, relies on the config for its agrument values
     *
     * @throws IOException
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory,
            DataTreeBuilder treeBuilder)
        throws IOException
    {
        this(txnLogFactory, DEFAULT_TICK_TIME, -1, -1, treeBuilder,
                new ZKDatabase(txnLogFactory));
    }

    /**
     * get the zookeeper database for this server
     * @return the zookeeper database for this server
     */
    public ZKDatabase getZKDatabase() {
        return this.zkDb;
    }
    
    /**
     * set the zkdatabase for this zookeeper server
     * @param zkDb
     */
    public void setZKDatabase(ZKDatabase zkDb) {
       this.zkDb = zkDb;
    }
    
    /**
     *  Restore sessions and data
     *  该方法别两个地方调用
     *  1.ZookeeperServer启动 {@link ZooKeeperServer#startdata()}
     *  2.集群模式下当前zk服务成为leader节点{@link Leader#lead()}
     */
    public void loadData() throws IOException, InterruptedException {
        /*
         * When a new leader starts executing Leader#lead, it 
         * invokes this method. The database, however, has been
         * initialized before running leader election so that
         * the server could pick its zxid for its initial vote.
         * It does it by invoking QuorumPeer#getLastLoggedZxid.
         * Consequently, we don't need to initialize it once more
         * and avoid the penalty of loading it a second time. Not 
         * reloading it is particularly important for applications
         * that host a large database.
         * 
         * The following if block checks whether the database has
         * been initialized or not. Note that this method is
         * invoked by at least one other method: 
         * ZooKeeperServer#startdata.
         *  
         * See ZOOKEEPER-1642 for more detail.
         */
        // zkDb内存数据库已初始化(说明此事当前zk服务成为了leader节点)
        if(zkDb.isInitialized()){
            // 加载内存中最后处理的zxid,然后赋值给属性hzxid
            setZxid(zkDb.getDataTreeLastProcessedZxid());
        }
        // zkDb内存数据库未初始化,说明当前zk服务是启动(包括重启)
        else {
            // 加载磁盘上的数据,然后返回zxid赋值给属性hzxid
            // 同时内部操作会设置zkDb的initialized属性为true
            setZxid(zkDb.loadDataBase());
        }
        
        // Clean up dead sessions
        // 遍历获取过期的会话Session
        LinkedList<Long> deadSessions = new LinkedList<Long>();
        for (Long session : zkDb.getSessions()) {
            if (zkDb.getSessionWithTimeOuts().get(session) == null) {
                deadSessions.add(session);
            }
        }
        // 更新dataTree中的initialized为true
        zkDb.setDataTreeInit(true);
        // 清除过期的Session
        for (long session : deadSessions) {
            // XXX: Is lastProcessedZxid really the best thing to use?
            // 终止session后,对应关联到该session的临时节点触发事件以及删除
            killSession(session, zkDb.getDataTreeLastProcessedZxid());
        }
    }

    /**
     * 生成内部目录树和会话的数据快照文件到磁盘
     */
    public void takeSnapshot(){

        try {
            txnLogFactory.save(zkDb.getDataTree(), zkDb.getSessionWithTimeOuts());
        } catch (IOException e) {
            LOG.error("Severe unrecoverable error, exiting", e);
            // This is a severe error that we cannot recover from,
            // so we need to exit
            System.exit(10);
        }
    }

  
    /**
     * This should be called from a synchronized block on this!
     */
    public long getZxid() {
        return hzxid.get();
    }

    long getNextZxid() {
        return hzxid.incrementAndGet();
    }

    public void setZxid(long zxid) {
        hzxid.set(zxid);
    }

    private void close(long sessionId) {
        submitRequest(null, sessionId, OpCode.closeSession, 0, null, null);
    }
    
    public void closeSession(long sessionId) {
        LOG.info("Closing session 0x" + Long.toHexString(sessionId));
        
        // we do not want to wait for a session close. send it as soon as we
        // detect it!
        close(sessionId);
    }

    // 清除过期的session
    protected void killSession(long sessionId, long zxid) {
        // 清除内存数据库中的临时会话记录
        zkDb.killSession(sessionId, zxid);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                                         "ZooKeeperServer --- killSession: 0x"
                    + Long.toHexString(sessionId));
        }
        // 去清除会话跟踪器中的记录
        if (sessionTracker != null) {
            sessionTracker.removeSession(sessionId);
        }
    }

    public void expire(Session session) {
        long sessionId = session.getSessionId();
        LOG.info("Expiring session 0x" + Long.toHexString(sessionId)
                + ", timeout of " + session.getTimeout() + "ms exceeded");
        close(sessionId);
    }

    public static class MissingSessionException extends IOException {
        private static final long serialVersionUID = 7467414635467261007L;

        public MissingSessionException(String msg) {
            super(msg);
        }
    }

    // 更新一下session
    void touch(ServerCnxn cnxn) throws MissingSessionException {
        if (cnxn == null) {
            return;
        }
        // 获取sessionid和超时时间
        long id = cnxn.getSessionId();
        int to = cnxn.getSessionTimeout();
        // touch session
        if (!sessionTracker.touchSession(id, to)) {
            throw new MissingSessionException(
                    "No session with sessionid 0x" + Long.toHexString(id)
                    + " exists, probably expired and removed");
        }
    }

    protected void registerJMX() {
        // register with JMX
        try {
            jmxServerBean = new ZooKeeperServerBean(this);
            MBeanRegistry.getInstance().register(jmxServerBean, null);
            
            try {
                jmxDataTreeBean = new DataTreeBean(zkDb.getDataTree());
                MBeanRegistry.getInstance().register(jmxDataTreeBean, jmxServerBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
                jmxDataTreeBean = null;
            }
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxServerBean = null;
        }
    }

    /**
     * 初始化ZKDatabase
     * @throws IOException
     * @throws InterruptedException
     */
    public void startdata() throws IOException, InterruptedException {
        //check to see if zkDb is not null
        // 初始化内存数据库ZKDatabase
        if (zkDb == null) {
            zkDb = new ZKDatabase(this.txnLogFactory);
        }
        // ZKDatabase首次初始化
        if (!zkDb.isInitialized()) {
            loadData();
        }
    }

    // 启动zk服务器
    public synchronized void startup() {
        // 创建会话Session管理器,默认SessionTrackerImpl
        if (sessionTracker == null) {
            createSessionTracker();
        }
        // 启动会话Session管理器
        // 其实就是将过去的session关闭
        startSessionTracker();
        // 初始化请求处理链
        setupRequestProcessors();

        registerJMX();
        // 设置当前zk服务状态为RUNNING
        // 默认状态为INITIAL
        setState(State.RUNNING);
        // todo 这里是通知哪里?
        // 通知的是org.apache.zookeeper.server.ZooKeeperServer.submitRequest()方法
        // 在启动的过程中由于NIOServerCnxnFactory先创建并启动,那么如果此时客户端发来相关事件请求
        // 由于ZooKeeperServer还未启动完毕,所以在将请求提交给请求处理链时会被阻塞wait(1000)住,
        // 这里就是初始化完毕防止这种情况,唤醒等待
        notifyAll();
    }
    // 默认处理链条
    protected void setupRequestProcessors() {
        // PrepRequestProcessor -> SyncRequestProcessor -> FinalRequestProcessor
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        RequestProcessor syncProcessor = new SyncRequestProcessor(this,
                finalProcessor);
        ((SyncRequestProcessor)syncProcessor).start();
        firstProcessor = new PrepRequestProcessor(this, syncProcessor);
        ((PrepRequestProcessor)firstProcessor).start();
    }

    public ZooKeeperServerListener getZooKeeperServerListener() {
        return listener;
    }

    // 创建会话跟踪器(leader/learner zk服务器会覆盖该方法)
    protected void createSessionTracker() {
        // 创建会话跟踪器,第四个参数sid默认为1也就是单机版的实现
        // 集群版本中各个服务器会传入自己的self.getId()
        sessionTracker = new SessionTrackerImpl(this, zkDb.getSessionWithTimeOuts(),
                tickTime, 1, getZooKeeperServerListener());
    }
    
    protected void startSessionTracker() {
        ((SessionTrackerImpl)sessionTracker).start();
    }

    /**
     * Sets the state of ZooKeeper server. After changing the state, it notifies
     * the server state change to a registered shutdown handler, if any.
     * <p>
     * The following are the server state transitions:
     * <li>During startup the server will be in the INITIAL state.</li>
     * <li>After successfully starting, the server sets the state to RUNNING.
     * </li>
     * <li>The server transitions to the ERROR state if it hits an internal
     * error. {@link ZooKeeperServerListenerImpl} notifies any critical resource
     * error events, e.g., SyncRequestProcessor not being able to write a txn to
     * disk.</li>
     * <li>During shutdown the server sets the state to SHUTDOWN, which
     * corresponds to the server not running.</li>
     *
     * @param state new server state.
     */
    // 设置服务器状态
    protected void setState(State state) {
        this.state = state;
        // Notify server state changes to the registered shutdown handler, if any.
        if (zkShutdownHandler != null) {
            zkShutdownHandler.handle(state);
        } else {
            LOG.debug("ZKShutdownHandler is not registered, so ZooKeeper server "
                    + "won't take any action on ERROR or SHUTDOWN server state changes");
        }
    }

    /**
     * This can be used while shutting down the server to see whether the server
     * is already shutdown or not.
     *
     * @return true if the server is running or server hits an error, false
     *         otherwise.
     */
    protected boolean canShutdown() {
        return state == State.RUNNING || state == State.ERROR;
    }

    public boolean isRunning() {
        return state == State.RUNNING;
    }

    public void shutdown() {
        shutdown(false);
    }

    /**
     * Shut down the server instance
     * @param fullyShutDown true if another server using the same database will not replace this one in the same process
     */
    public synchronized void shutdown(boolean fullyShutDown) {
        if (!canShutdown()) {
            LOG.debug("ZooKeeper server is not running, so not proceeding to shutdown!");
            return;
        }
        LOG.info("shutting down");

        // new RuntimeException("Calling shutdown").printStackTrace();
        setState(State.SHUTDOWN);
        // Since sessionTracker and syncThreads poll we just have to
        // set running to false and they will detect it during the poll
        // interval.
        if (sessionTracker != null) {
            sessionTracker.shutdown();
        }
        if (firstProcessor != null) {
            firstProcessor.shutdown();
        }

        if (zkDb != null) {
            if (fullyShutDown) {
                zkDb.clear();
            } else {
                // else there is no need to clear the database
                //  * When a new quorum is established we can still apply the diff
                //    on top of the same zkDb data
                //  * If we fetch a new snapshot from leader, the zkDb will be
                //    cleared anyway before loading the snapshot
                try {
                    //This will fast forward the database to the latest recorded transactions
                    zkDb.fastForwardDataBase();
                } catch (IOException e) {
                    LOG.error("Error updating DB", e);
                    zkDb.clear();
                }
            }
        }

        unregisterJMX();
    }

    protected void unregisterJMX() {
        // unregister from JMX
        try {
            if (jmxDataTreeBean != null) {
                MBeanRegistry.getInstance().unregister(jmxDataTreeBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        try {
            if (jmxServerBean != null) {
                MBeanRegistry.getInstance().unregister(jmxServerBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxServerBean = null;
        jmxDataTreeBean = null;
    }

    /**
     * 新增正在处理的请求数
     * 参考{@link ZooKeeperServer#submitRequest(org.apache.zookeeper.server.Request)}
     */
    public void incInProcess() {
        requestsInProcess.incrementAndGet();
    }

    public void decInProcess() {
        requestsInProcess.decrementAndGet();
    }

    public int getInProcess() {
        return requestsInProcess.get();
    }

    /**
     * This structure is used to facilitate information sharing between PrepRP
     * and FinalRP.
     */
    // 这个类用于PrepRequestProcessor和FinalRequestProcessor的信息共享
    static class ChangeRecord {
        ChangeRecord(long zxid, String path, StatPersisted stat, int childCount,
                List<ACL> acl) {
            this.zxid = zxid;
            this.path = path;
            this.stat = stat;
            this.childCount = childCount;
            this.acl = acl;
        }

        long zxid;

        String path;

        StatPersisted stat; /* Make sure to create a new object when changing */

        int childCount;

        List<ACL> acl; /* Make sure to create a new object when changing */

        @SuppressWarnings("unchecked")
        ChangeRecord duplicate(long zxid) {
            StatPersisted stat = new StatPersisted();
            if (this.stat != null) {
                DataTree.copyStatPersisted(this.stat, stat);
            }
            return new ChangeRecord(zxid, path, stat, childCount,
                    acl == null ? new ArrayList<ACL>() : new ArrayList<ACL>(acl));
        }
    }

    // 基于id生成密码
    byte[] generatePasswd(long id) {
        Random r = new Random(id ^ superSecret);
        byte p[] = new byte[16];
        r.nextBytes(p);
        return p;
    }

    // 校验sessionId和password是否正确
    protected boolean checkPasswd(long sessionId, byte[] passwd) {
        // sessionId不为0 && passwd和计算出来的一致
        return sessionId != 0
                && Arrays.equals(passwd, generatePasswd(sessionId));
    }
    // 创建session
    // cnxn是针对当前客户端的NIOServerCnxn
    long createSession(ServerCnxn cnxn, byte passwd[], int timeout) {
        // 创建sessionId,以及对应的SessionImpl
        long sessionId = sessionTracker.createSession(timeout);
        // 生成随机密码
        Random r = new Random(sessionId ^ superSecret);
        r.nextBytes(passwd);
        ByteBuffer to = ByteBuffer.allocate(4);
        to.putInt(timeout);
        // 设置sessionId到ServerCnxn
        cnxn.setSessionId(sessionId);
        // 封装Request准备递交给责任链来处理
        submitRequest(cnxn, sessionId, OpCode.createSession, 0, to, null);
        return sessionId;
    }

    /**
     * set the owner of this session as owner
     * @param id the session id
     * @param owner the owner of the session
     * @throws SessionExpiredException
     */
    public void setOwner(long id, Object owner) throws SessionExpiredException {
        sessionTracker.setOwner(id, owner);
    }

    // 激活session
    protected void revalidateSession(ServerCnxn cnxn, long sessionId,
            int sessionTimeout) throws IOException {
        // 调用sessionTracker中的方法，激活该sessionId并返回会话跟踪器是否有对应记录
        boolean rc = sessionTracker.touchSession(sessionId, sessionTimeout);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,ZooTrace.SESSION_TRACE_MASK,
                                     "Session 0x" + Long.toHexString(sessionId) +
                    " is valid: " + rc);
        }
        // 完成session的初始化，注意第二个参数一般为true，如果上一步touchSession失败则为false
        finishSessionInit(cnxn, rc);
    }

    // 会话session重连
    public void reopenSession(ServerCnxn cnxn, long sessionId, byte[] passwd,
            int sessionTimeout) throws IOException {
        // 校验passwd
        if (!checkPasswd(sessionId, passwd)) {
            // 校验passwd失败,第二个参数为false
            finishSessionInit(cnxn, false);
        } else {
            // 校验passwd成功
            revalidateSession(cnxn, sessionId, sessionTimeout);
        }
    }

    // 完成session初始化
    // 参数valid代表了会话是否验证通过
    public void finishSessionInit(ServerCnxn cnxn, boolean valid) {
        // register with JMX
        try {
            // 如果是有效的session了，那么注册cnxn
            if (valid) {
                // 初始化并注册一个ConnectionBean
                serverCnxnFactory.registerConnection(cnxn);
            }
        } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
        }

        try {
            // 创建响应
            ConnectResponse rsp = new ConnectResponse(0, valid ? cnxn.getSessionTimeout()
                    : 0, valid ? cnxn.getSessionId() : 0, // send 0 if session is no
                            // longer valid
                            valid ? generatePasswd(cnxn.getSessionId()) : new byte[16]);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
            bos.writeInt(-1, "len");
            rsp.serialize(bos, "connect");
            if (!cnxn.isOldClient) {
                bos.writeBool(
                        this instanceof ReadOnlyZooKeeperServer, "readOnly");
            }
            baos.close();
            ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
            bb.putInt(bb.remaining() - 4).rewind();
            // 发送响应
            cnxn.sendBuffer(bb);    
            // session创建失败
            if (!valid) {
                LOG.info("Invalid session 0x"
                        + Long.toHexString(cnxn.getSessionId())
                        + " for client "
                        + cnxn.getRemoteSocketAddress()
                        + ", probably expired");
                // 发送closeConn
                cnxn.sendBuffer(ServerCnxnFactory.closeConn);
            // session创建成功
            } else {
                LOG.info("Established session 0x"
                        + Long.toHexString(cnxn.getSessionId())
                        + " with negotiated timeout " + cnxn.getSessionTimeout()
                        + " for client "
                        + cnxn.getRemoteSocketAddress());
                // 注册OP_READ事件
                cnxn.enableRecv();
            }
                
        } catch (Exception e) {
            LOG.warn("Exception while establishing session, closing", e);
            cnxn.close();
        }
    }
    
    public void closeSession(ServerCnxn cnxn, RequestHeader requestHeader) {
        closeSession(cnxn.getSessionId());
    }

    public long getServerId() {
        return 0;
    }

    /**
     * 提交请求
     * @param cnxn 对应当前客户端的ServerCnxn
     * @param sessionId 客户端的sessionId
     * @param xid 0
     * @param bb 4个字节记录了session的超时时间
     * @param type 提交的请求类型
     * @param authInfo
     */
    private void submitRequest(ServerCnxn cnxn, long sessionId, int type,
                                    int xid, ByteBuffer bb, List<Id> authInfo) {
        // 创建一个在责任链中传递的请求
        Request si = new Request(cnxn, sessionId, xid, type, bb, authInfo);
        submitRequest(si);
    }

    // 如果是单机模式,走ZooKeeperServer,执行顺序为PrepRequestProcessor -> SyncRequestProcessor -> finalProcessor
    // 如果是集群模式,走不通的Server,执行顺序也不一样
    public void submitRequest(Request si) {
        // 由于NIOServerCnxnFactory先启动,那么如果此时有客户端请求,就一定处理,由于ZookeeperServer还未完成责任链的初始化
        // 所有此时firstProcessor可能为null,那么就需要等待责任链的初始化
        // 这也是为什么在org.apache.zookeeper.server.ZooKeeperServer#startup最后一行代码需要notifyAll()的原因
        if (firstProcessor == null) {
            synchronized (this) {
                try {
                    // Since all requests are passed to the request
                    // processor it should wait for setting up the request
                    // processor chain. The state will be updated to RUNNING
                    // after the setup.
                    while (state == State.INITIAL) {
                        wait(1000);
                    }
                } catch (InterruptedException e) {
                    LOG.warn("Unexpected interruption", e);
                }
                if (firstProcessor == null || state != State.RUNNING) {
                    throw new RuntimeException("Not started");
                }
            }
        }
        try {
            // 更新session
            touch(si.cnxn);
            // 验证请求类型
            boolean validpacket = Request.isValid(si.type);
            if (validpacket) {
                // 子类会覆盖firstProcessor的初始化
                // 如果是ZooKeeperServer,执行顺序为PrepRequestProcessor -> SyncRequestProcessor -> finalProcessor
                // 如果是ObserverZooKeeperServer,执行顺序为ObserverRequestProcessor->CommitProcessor->FinalRequestProcessor
                // 如果是FollowerZookeeperServer,执行顺序为FollowerRequestProcessor -> CommitProcessor -> FinalRequestProcessor
                // 如果是LeaderZookeeperServer,执行顺序为PrepRequestProcessor -> ProposalRequestProcessor -> CommitProcessor -> ToBeAppliedRequestProcessor -> FinalRequestProcessor
                firstProcessor.processRequest(si);
                if (si.cnxn != null) {
                    incInProcess();
                }
            } else {
                LOG.warn("Received packet at server of unknown type " + si.type);
                new UnimplementedRequestProcessor().processRequest(si);
            }
        } catch (MissingSessionException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Dropping request: " + e.getMessage());
            }
        } catch (RequestProcessorException e) {
            LOG.error("Unable to process request:" + e.getMessage(), e);
        }
    }

    public static int getSnapCount() {
        String sc = System.getProperty("zookeeper.snapCount");
        try {
            int snapCount = Integer.parseInt(sc);

            // snapCount must be 2 or more. See org.apache.zookeeper.server.SyncRequestProcessor
            if( snapCount < 2 ) {
                LOG.warn("SnapCount should be 2 or more. Now, snapCount is reset to 2");
                snapCount = 2;
            }
            return snapCount;
        } catch (Exception e) {
            return 100000;
        }
    }

    public int getGlobalOutstandingLimit() {
        String sc = System.getProperty("zookeeper.globalOutstandingLimit");
        int limit;
        try {
            limit = Integer.parseInt(sc);
        } catch (Exception e) {
            limit = 1000;
        }
        return limit;
    }

    public void setServerCnxnFactory(ServerCnxnFactory factory) {
        serverCnxnFactory = factory;
    }

    public ServerCnxnFactory getServerCnxnFactory() {
        return serverCnxnFactory;
    }

    /**
     * return the last proceesed id from the 
     * datatree
     */
    public long getLastProcessedZxid() {
        return zkDb.getDataTreeLastProcessedZxid();
    }

    /**
     * return the outstanding requests
     * in the queue, which havent been 
     * processed yet
     */
    public long getOutstandingRequests() {
        return getInProcess();
    }

    /**
     * trunccate the log to get in sync with others 
     * if in a quorum
     * @param zxid the zxid that it needs to get in sync
     * with others
     * @throws IOException
     */
    public void truncateLog(long zxid) throws IOException {
        this.zkDb.truncateLog(zxid);
    }
       
    public int getTickTime() {
        return tickTime;
    }

    public void setTickTime(int tickTime) {
        LOG.info("tickTime set to " + tickTime);
        this.tickTime = tickTime;
    }

    public int getMinSessionTimeout() {
        return minSessionTimeout == -1 ? tickTime * 2 : minSessionTimeout;
    }

    public void setMinSessionTimeout(int min) {
        LOG.info("minSessionTimeout set to " + min);
        this.minSessionTimeout = min;
    }

    public int getMaxSessionTimeout() {
        return maxSessionTimeout == -1 ? tickTime * 20 : maxSessionTimeout;
    }

    public void setMaxSessionTimeout(int max) {
        LOG.info("maxSessionTimeout set to " + max);
        this.maxSessionTimeout = max;
    }

    public int getClientPort() {
        return serverCnxnFactory != null ? serverCnxnFactory.getLocalPort() : -1;
    }

    public void setTxnLogFactory(FileTxnSnapLog txnLog) {
        this.txnLogFactory = txnLog;
    }
    
    public FileTxnSnapLog getTxnLogFactory() {
        return this.txnLogFactory;
    }

    public String getState() {
        return "standalone";
    }

    public void dumpEphemerals(PrintWriter pwriter) {
    	zkDb.dumpEphemerals(pwriter);
    }
    
    /**
     * return the total number of client connections that are alive
     * to this server
     */
    public int getNumAliveConnections() {
        return serverCnxnFactory.getNumAliveConnections();
    }

    // 处理ConnectRequest,开始建立会话
    // cnxn是针对当前客户端的NIOServerCnxn
    public void processConnectRequest(ServerCnxn cnxn, ByteBuffer incomingBuffer) throws IOException {
        // 包装数据到BinaryInputArchive中
        BinaryInputArchive bia = BinaryInputArchive.getArchive(new ByteBufferInputStream(incomingBuffer));
        // 解析客户端发送过来的ConnectRequest
        ConnectRequest connReq = new ConnectRequest();
        connReq.deserialize(bia, "connect");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Session establishment request from client "
                    + cnxn.getRemoteSocketAddress()
                    + " client's lastZxid is 0x"
                    + Long.toHexString(connReq.getLastZxidSeen()));
        }
        boolean readOnly = false;
        try {
            readOnly = bia.readBool("readOnly");
            cnxn.isOldClient = false;
        } catch (IOException e) {
            // this is ok -- just a packet from an old client which
            // doesn't contain readOnly field
            LOG.warn("Connection request from old client "
                    + cnxn.getRemoteSocketAddress()
                    + "; will be dropped if server is in r-o mode");
        }
        if (readOnly == false && this instanceof ReadOnlyZooKeeperServer) {
            String msg = "Refusing session request for not-read-only client "
                + cnxn.getRemoteSocketAddress();
            LOG.info(msg);
            throw new CloseRequestException(msg);
        }
        if (connReq.getLastZxidSeen() > zkDb.dataTree.lastProcessedZxid) {
            String msg = "Refusing session request for client "
                + cnxn.getRemoteSocketAddress()
                + " as it has seen zxid 0x"
                + Long.toHexString(connReq.getLastZxidSeen())
                + " our last zxid is 0x"
                + Long.toHexString(getZKDatabase().getDataTreeLastProcessedZxid())
                + " client must try another server";

            LOG.info(msg);
            throw new CloseRequestException(msg);
        }
        int sessionTimeout = connReq.getTimeOut();
        byte passwd[] = connReq.getPasswd();
        int minSessionTimeout = getMinSessionTimeout();
        if (sessionTimeout < minSessionTimeout) {
            sessionTimeout = minSessionTimeout;
        }
        int maxSessionTimeout = getMaxSessionTimeout();
        if (sessionTimeout > maxSessionTimeout) {
            sessionTimeout = maxSessionTimeout;
        }
        cnxn.setSessionTimeout(sessionTimeout);
        // We don't want to receive any packets until we are sure that the
        // session is setup
        // 取消监听OP_READ事件，因为还没有建立会话
        cnxn.disableRecv();
        // 获取请求中的sessionID
        long sessionId = connReq.getSessionId();
        if (sessionId != 0) {
            long clientSessionId = connReq.getSessionId();
            LOG.info("Client attempting to renew session 0x"
                    + Long.toHexString(clientSessionId)
                    + " at " + cnxn.getRemoteSocketAddress());
            // 关闭旧的cnxn
            // 1.删除sessionMap中记录的sessionId和NIOServerCnxn的对应关系
            // 2.调用NIOServerCnxn.close()方法关闭旧的NIOServerCnxn
            //      删除NIOServerCnxnFactory中记录的关于当前客户端的session和NIOServerCnxn
            //      删除当前客户端对应的事件
            //      关闭socket
            serverCnxnFactory.closeSession(sessionId);
            // 设置cnxn的sesionId，这里底层涉及两个步骤：
            // 1.设置sessionId到serverCnxn
            // 2.设置serverCnxn到serverCnxnFactory的sessionMap中
            cnxn.setSessionId(sessionId);
            // 重新开启session
            reopenSession(cnxn, sessionId, passwd, sessionTimeout);
        } else {
            LOG.info("Client attempting to establish new session at "
                    + cnxn.getRemoteSocketAddress());
            // 创建sessionId
            createSession(cnxn, passwd, sessionTimeout);
        }
    }

    public boolean shouldThrottle(long outStandingCount) {
        if (getGlobalOutstandingLimit() < getInProcess()) {
            return outStandingCount > 0;
        }
        return false; 
    }

    // 处理接受到的数据
    public void processPacket(ServerCnxn cnxn, ByteBuffer incomingBuffer) throws IOException {
        // We have the request, now process and setup for next
        // 处理数据，将数据incomingBuffer封装为一个BinaryInputArchive
        InputStream bais = new ByteBufferInputStream(incomingBuffer);
        BinaryInputArchive bia = BinaryInputArchive.getArchive(bais);
        // 解析出请求头RequestHeader
        RequestHeader h = new RequestHeader();
        h.deserialize(bia, "header");
        // Through the magic of byte buffers, txn will not be
        // pointing
        // to the start of the txn
        incomingBuffer = incomingBuffer.slice();
        /**
         * 参考{@link ClientCnxn#addAuthInfo(java.lang.String, byte[])}
         * 这里判断成功那么可以肯定客户端的操作是addAuthInfo()
         */
        if (h.getType() == OpCode.auth) {
            LOG.info("got auth packet " + cnxn.getRemoteSocketAddress());
            // 读取客户端数据到AuthPacket
            AuthPacket authPacket = new AuthPacket();
            ByteBufferInputStream.byteBuffer2Record(incomingBuffer, authPacket);
            // 获取客户端的授权模式digest,ip或自定义
            String scheme = authPacket.getScheme();
            // 获取对应的权限认证插件类
            AuthenticationProvider ap = ProviderRegistry.getProvider(scheme);
            // 返回响应code,默认权限认证失败
            Code authReturn = KeeperException.Code.AUTHFAILED;
            // 插件类不为空
            if(ap != null) {
                try {
                    // 将新增的授权模式添加到ServerCnxn的authInfo集合中
                    // 并重新赋值权限响应Code
                    authReturn = ap.handleAuthentication(cnxn, authPacket.getAuth());
                } catch(RuntimeException e) {
                    LOG.warn("Caught runtime exception from AuthenticationProvider: " + scheme + " due to " + e);
                    authReturn = KeeperException.Code.AUTHFAILED;                   
                }
            }
            // 添加权限失败
            if (authReturn!= KeeperException.Code.OK) {
                if (ap == null) {
                    LOG.warn("No authentication provider for scheme: "
                            + scheme + " has "
                            + ProviderRegistry.listProviders());
                } else {
                    LOG.warn("Authentication failed for scheme: " + scheme);
                }
                // send a response...
                // 新建响应并返回给客户端
                // 关闭session
                // 取消客户端的OP_READ事件监听
                // 响应头中分配的zxid为0
                ReplyHeader rh = new ReplyHeader(h.getXid(), 0,
                        KeeperException.Code.AUTHFAILED.intValue());
                cnxn.sendResponse(rh, null, null);
                // ... and close connection
                cnxn.sendBuffer(ServerCnxnFactory.closeConn);
                cnxn.disableRecv();
            // 添加权限成功
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Authentication succeeded for scheme: "
                              + scheme);
                }
                LOG.info("auth success " + cnxn.getRemoteSocketAddress());
                // 封装响应头
                ReplyHeader rh = new ReplyHeader(h.getXid(), 0,
                        KeeperException.Code.OK.intValue());
                cnxn.sendResponse(rh, null, null);
            }
            return;
        // 非AUTH请求头
        } else {
            if (h.getType() == OpCode.sasl) {
                Record rsp = processSasl(incomingBuffer,cnxn);
                ReplyHeader rh = new ReplyHeader(h.getXid(), 0, KeeperException.Code.OK.intValue());
                cnxn.sendResponse(rh,rsp, "response"); // not sure about 3rd arg..what is it?
                return;
            }

            else {
                // 一个客户端对应一个NioServerCnxn
                // 这里会将客户端的sessionId/客户端的xid/当前客户端的权限信息封装到一个Request中
                Request si = new Request(cnxn, cnxn.getSessionId(), h.getXid(),
                  h.getType(), incomingBuffer, cnxn.getAuthInfo());
                /**
                 * 详见{@link SessionTrackerImpl#checkSession(long, java.lang.Object)}
                 * 注意这里设置了Reuqest的owner属性
                 */
                si.setOwner(ServerCnxn.me);
                // 提交请求
                submitRequest(si);
            }
        }
        // 注意这里
        // 递增接受到的事务请求数(还未处理的,处理后会递减)
        // 如果超过阈值就取消OP_READ事件
        cnxn.incrOutstandingRequests(h);
    }

    private Record processSasl(ByteBuffer incomingBuffer, ServerCnxn cnxn) throws IOException {
        LOG.debug("Responding to client SASL token.");
        GetSASLRequest clientTokenRecord = new GetSASLRequest();
        ByteBufferInputStream.byteBuffer2Record(incomingBuffer,clientTokenRecord);
        byte[] clientToken = clientTokenRecord.getToken();
        LOG.debug("Size of client SASL token: " + clientToken.length);
        byte[] responseToken = null;
        try {
            ZooKeeperSaslServer saslServer  = cnxn.zooKeeperSaslServer;
            try {
                // note that clientToken might be empty (clientToken.length == 0):
                // if using the DIGEST-MD5 mechanism, clientToken will be empty at the beginning of the
                // SASL negotiation process.
                responseToken = saslServer.evaluateResponse(clientToken);
                if (saslServer.isComplete() == true) {
                    String authorizationID = saslServer.getAuthorizationID();
                    LOG.info("adding SASL authorization for authorizationID: " + authorizationID);
                    cnxn.addAuthInfo(new Id("sasl",authorizationID));
                }
            }
            catch (SaslException e) {
                LOG.warn("Client failed to SASL authenticate: " + e, e);
                if ((System.getProperty("zookeeper.allowSaslFailedClients") != null)
                  &&
                  (System.getProperty("zookeeper.allowSaslFailedClients").equals("true"))) {
                    LOG.warn("Maintaining client connection despite SASL authentication failure.");
                } else {
                    LOG.warn("Closing client connection due to SASL authentication failure.");
                    cnxn.close();
                }
            }
        }
        catch (NullPointerException e) {
            LOG.error("cnxn.saslServer is null: cnxn object did not initialize its saslServer properly.");
        }
        if (responseToken != null) {
            LOG.debug("Size of server SASL response: " + responseToken.length);
        }
        // wrap SASL response token to client inside a Response object.
        return new SetSASLResponse(responseToken);
    }
    
    public ProcessTxnResult processTxn(TxnHeader hdr, Record txn) {
        ProcessTxnResult rc;
        int opCode = hdr.getType();
        long sessionId = hdr.getClientId();
        rc = getZKDatabase().processTxn(hdr, txn);
        // 如果是创建session请求
        if (opCode == OpCode.createSession) {
            if (txn instanceof CreateSessionTxn) {
                CreateSessionTxn cst = (CreateSessionTxn) txn;
                sessionTracker.addSession(sessionId, cst
                        .getTimeOut());
            } else {
                LOG.warn("*****>>>>> Got "
                        + txn.getClass() + " "
                        + txn.toString());
            }
        // 如果是关闭closeSession请求
        } else if (opCode == OpCode.closeSession) {
            sessionTracker.removeSession(sessionId);
        }
        return rc;
    }

    /**
     * This method is used to register the ZooKeeperServerShutdownHandler to get
     * server's error or shutdown state change notifications.
     * {@link ZooKeeperServerShutdownHandler#handle(State)} will be called for
     * every server state changes {@link #setState(State)}.
     *
     * @param zkShutdownHandler shutdown handler
     */
    void registerServerShutdownHandler(ZooKeeperServerShutdownHandler zkShutdownHandler) {
        this.zkShutdownHandler = zkShutdownHandler;
    }
}
