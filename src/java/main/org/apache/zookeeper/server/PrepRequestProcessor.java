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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jute.Record;
import org.apache.jute.BinaryOutputArchive;

import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadArgumentsException;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.Txn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * This request processor is generally at the start of a RequestProcessor
 * change. It sets up any transactions associated with requests that change the
 * state of the system. It counts on ZooKeeperServer to update
 * outstandingRequests, so that it can take into account transactions that are
 * in the queue to be applied when generating a transaction.
 */

/**
 * PrepRequestProcessor位于处理器链条的开头
 * 继续了ZooKeeperCriticalThread，而ZooKeeperCriticalThread继承了ZooKeeperThread，ZooKeeperThread又继承了Thread，
 * 所以PrepRequestProcessor可以当一个线程使用
 */
public class PrepRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PrepRequestProcessor.class);

    static boolean skipACL;
    static {
        skipACL = System.getProperty("zookeeper.skipACL", "no").equals("yes");
        if (skipACL) {
            LOG.info("zookeeper.skipACL==\"yes\", ACL checks will be skipped");
        }
    }

    /**
     * this is only for testing purposes.
     * should never be useed otherwise
     */
    private static  boolean failCreate = false;
    // 记录已提交的请求
    LinkedBlockingQueue<Request> submittedRequests = new LinkedBlockingQueue<Request>();
    // 下一个处理器
    RequestProcessor nextProcessor;
    // zk服务器
    ZooKeeperServer zks;

    // 初始化PrepRequestProcessor
    public PrepRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        // 最后一个参数获取zk服务器中的listener交给父类ZooKeeperCriticalThread
        super("ProcessThread(sid:" + zks.getServerId() + " cport:"
                + zks.getClientPort() + "):", zks.getZooKeeperServerListener());
        // 下一个处理器
        this.nextProcessor = nextProcessor;
        // zk服务器
        this.zks = zks;
    }

    /**
     * method for tests to set failCreate
     * @param b
     */
    public static void setFailCreate(boolean b) {
        failCreate = b;
    }

    /**
     * 可以看出PrepRequestProcessor被当成了一个Thread来使用，内部不断的while循环处理请求
     */
    @Override
    public void run() {
        try {
            while (true) {
                // 获取已提交的请求
                Request request = submittedRequests.take();
                long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
                if (request.type == OpCode.ping) {
                    traceMask = ZooTrace.CLIENT_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, traceMask, 'P', request, "");
                }
                // 表示当前处理器已经关闭，不再处理请求(参考shutdown()方法)
                if (Request.requestOfDeath == request) {
                    break;
                }
                // 开始处理请求
                pRequest(request);
            }
        } catch (RequestProcessorException e) {
            if (e.getCause() instanceof XidRolloverException) {
                LOG.info(e.getCause().getMessage());
            }
            // 发生了异常
            handleException(this.getName(), e);
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("PrepRequestProcessor exited loop!");
    }

    // 根据path获取对应节点的ChangeRecord
    ChangeRecord getRecordForPath(String path) throws KeeperException.NoNodeException {
        ChangeRecord lastChange = null;
        synchronized (zks.outstandingChanges) {
            lastChange = zks.outstandingChangesForPath.get(path);
            if (lastChange == null) {
                // 从内存数据库中查找父节点
                DataNode n = zks.getZKDatabase().getNode(path);
                if (n != null) {
                    Set<String> children;
                    synchronized(n) {
                        children = n.getChildren();
                    }
                    lastChange = new ChangeRecord(-1, path, n.stat, children.size(),
                            zks.getZKDatabase().aclForNode(n));
                }
            }
        }
        // 父节点没找到报错
        if (lastChange == null || lastChange.stat == null) {
            throw new KeeperException.NoNodeException(path);
        }
        // 返回父节点
        return lastChange;
    }

    private ChangeRecord getOutstandingChange(String path) {
        synchronized (zks.outstandingChanges) {
            return zks.outstandingChangesForPath.get(path);
        }
    }

    void addChangeRecord(ChangeRecord c) {
        synchronized (zks.outstandingChanges) {
            zks.outstandingChanges.add(c);
            zks.outstandingChangesForPath.put(c.path, c);
        }
    }

    /**
     * Grab current pending change records for each op in a multi-op.
     * 
     * This is used inside MultiOp error code path to rollback in the event
     * of a failed multi-op.
     *
     * @param multiRequest
     * @return a map that contains previously existed records that probably need to be
     *         rolled back in any failure.
     */
    // 根据multiRequest中的请求路径和请求路径的父路径或者在当前操作前已经变更的记录
    // 用于处理multiRequest出现异常时的回滚
    HashMap<String, ChangeRecord> getPendingChanges(MultiTransactionRecord multiRequest) {
        HashMap<String, ChangeRecord> pendingChangeRecords = new HashMap<String, ChangeRecord>();
        // 获取multiRequest要操作的路径对应的ChangeRecord
        // 这些ChangeRecord此时还未写入事务日志文件
        for (Op op : multiRequest) {
            String path = op.getPath();
            ChangeRecord cr = getOutstandingChange(path);
            // only previously existing records need to be rolled back.
            if (cr != null) {
                pendingChangeRecords.put(path, cr);
            }

            /*
             * ZOOKEEPER-1624 - We need to store for parent's ChangeRecord
             * of the parent node of a request. So that if this is a
             * sequential node creation request, rollbackPendingChanges()
             * can restore previous parent's ChangeRecord correctly.
             *
             * Otherwise, sequential node name generation will be incorrect
             * for a subsequent request.
             */
            // 同理获取对应的父节点
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash == -1 || path.indexOf('\0') != -1) {
                continue;
            }
            String parentPath = path.substring(0, lastSlash);
            ChangeRecord parentCr = getOutstandingChange(parentPath);
            if (parentCr != null) {
                pendingChangeRecords.put(parentPath, parentCr);
            }
        }

        return pendingChangeRecords;
    }

    /**
     * Rollback pending changes records from a failed multi-op.
     *
     * If a multi-op fails, we can't leave any invalid change records we created
     * around. We also need to restore their prior value (if any) if their prior
     * value is still valid.
     *
     * @param zxid
     * @param pendingChangeRecords 在进行事务操作前,已经处理的记录
     */
    void rollbackPendingChanges(long zxid, HashMap<String, ChangeRecord>pendingChangeRecords) {
        synchronized (zks.outstandingChanges) {
            // Grab a list iterator starting at the END of the list so we can iterate in reverse
            // 遍历所有暂存的记录,然后删除zxid等于参数zxid的ChangeRecord
            ListIterator<ChangeRecord> iter = zks.outstandingChanges.listIterator(zks.outstandingChanges.size());
            while (iter.hasPrevious()) {
                ChangeRecord c = iter.previous();
                if (c.zxid == zxid) {
                    iter.remove();
                    // Remove all outstanding changes for paths of this multi.
                    // Previous records will be added back later.
                    zks.outstandingChangesForPath.remove(c.path);
                } else {
                    break;
                }
            }

            // we don't need to roll back any records because there is nothing left.
            // outstandingChanges为空说明其内部刚刚记录的数据都是针对当前操作产生的
            // 因为上面一步删除的是等于zxid的数据
            if (zks.outstandingChanges.isEmpty()) {
                return;
            }
            // 走到这里说明outstandingChanges还不为null,那么获取集合中最小的zxid
            long firstZxid = zks.outstandingChanges.get(0).zxid;

            for (ChangeRecord c : pendingChangeRecords.values()) {
                // Don't apply any prior change records less than firstZxid.
                // Note that previous outstanding requests might have been removed
                // once they are completed.
                // 需要回放记录的zxid小于当前集合中最小的zxid那么就不要处理了
                // 因为此时的c已经被删除了see org.apache.zookeeper.server.FinalRequestProcessor.processRequest
                if (c.zxid < firstZxid) {
                    continue;
                }

                // add previously existing records back.
                // 将以前存在的记录添加回去,此举是防止误删除记录
                zks.outstandingChangesForPath.put(c.path, c);
            }
        }
    }

    /**
     * 在进行操作前检查ACL是否有这个权限
     * @param zks zk服务实例
     * @param acl 操作路径父节点或当前路径节点的ACL
     * @param perm 当前的操作,参考{@link org.apache.zookeeper.ZooDefs.Perms}
     * @param ids 当前客户端ServerCnxn携带的权限信息
     * @throws KeeperException.NoAuthException
     */
    static void checkACL(ZooKeeperServer zks, List<ACL> acl, int perm,
            List<Id> ids) throws KeeperException.NoAuthException {
        // 读取配置zookeeper.skipACL跳过权限验证,默认true
        if (skipACL) {
            return;
        }
        // acl不存在
        if (acl == null || acl.size() == 0) {
            return;
        }
        // 客户端是超级管理员则直接返回运行操作
        for (Id authId : ids) {
            if (authId.getScheme().equals("super")) {
                return;
            }
        }
        // 遍历操作节点需要的权限并验证是否有对应的权限,没有则抛出异常
        for (ACL a : acl) {
            Id id = a.getId();
            if ((a.getPerms() & perm) != 0) {
                if (id.getScheme().equals("world")
                        && id.getId().equals("anyone")) {
                    return;
                }
                // 更新授权模式获取对应的插件对象
                AuthenticationProvider ap = ProviderRegistry.getProvider(id
                        .getScheme());
                if (ap != null) {
                    // 获取客户端ServerCnxn所拥有的权限
                    for (Id authId : ids) {
                        // 校验是否有对应的权限,如果有就直接返回
                        if (authId.getScheme().equals(id.getScheme())
                                && ap.matches(authId.getId(), id.getId())) {
                            return;
                        }
                    }
                }
            }
        }
        // 没授权抛出异常
        throw new KeeperException.NoAuthException();
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param type 请求类型
     * @param zxid zk服务的下一个zxid
     * @param request 请求
     * @param record
     */
    @SuppressWarnings("unchecked")
    protected void pRequest2Txn(int type, long zxid, Request request, Record record, boolean deserialize)
        throws KeeperException, IOException, RequestProcessorException
    {
        // 设置请求的hdr
        request.hdr = new TxnHeader(request.sessionId, request.cxid, zxid,
                                    Time.currentWallTime(), type);

        switch (type) {
            case OpCode.create:                
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                CreateRequest createRequest = (CreateRequest)record;   
                if(deserialize)
                    // 反序列化request.request数据到createRequest
                    ByteBufferInputStream.byteBuffer2Record(request.request, createRequest);
                // 1.获取创建的路径
                String path = createRequest.getPath();
                int lastSlash = path.lastIndexOf('/');
                if (lastSlash == -1 || path.indexOf('\0') != -1 || failCreate) {
                    LOG.info("Invalid path " + path + " with session 0x" +
                            Long.toHexString(request.sessionId));
                    throw new KeeperException.BadArgumentsException(path);
                }
                // 2.将请求中携带的ACL去重
                List<ACL> listACL = removeDuplicates(createRequest.getAcl());
                // 3.修正ACL
                if (!fixupACL(request.authInfo, listACL)) {
                    throw new KeeperException.InvalidACLException(path);
                }
                // 4.获取父节点路径
                String parentPath = path.substring(0, lastSlash);
                // 5.获取父节点的ChangeRecord
                ChangeRecord parentRecord = getRecordForPath(parentPath);
                // 6.校验ACL权限
                checkACL(zks, parentRecord.acl, ZooDefs.Perms.CREATE,
                        request.authInfo);
                int parentCVersion = parentRecord.stat.getCversion();
                // 7获取创建的模式
                CreateMode createMode =
                    CreateMode.fromFlag(createRequest.getFlags());
                // 7-1.如果是顺序节点重新定义path
                if (createMode.isSequential()) {
                    path = path + String.format(Locale.ENGLISH, "%010d", parentCVersion);
                }
                // 8.检查path路径是否包含特殊符号
                validatePath(path, request.sessionId);
                try {
                    // 9.路径已存在
                    if (getRecordForPath(path) != null) {
                        throw new KeeperException.NodeExistsException(path);
                    }
                } catch (KeeperException.NoNodeException e) {
                    // ignore this one
                }
                // 10.父节点是否为临时节点，这里可以看出临时节点下不可创建子节点
                boolean ephemeralParent = parentRecord.stat.getEphemeralOwner() != 0;
                if (ephemeralParent) {
                    throw new KeeperException.NoChildrenForEphemeralsException(path);
                }
                // 更新父节点的子节点版本号，也就是cversion,该值记录了子节点修改次数
                int newCversion = parentRecord.stat.getCversion()+1;
                // 生成txn
                request.txn = new CreateTxn(path, createRequest.getData(),
                        listACL,
                        createMode.isEphemeral(), newCversion);
                StatPersisted s = new StatPersisted();
                // 临时节点设置当前路径对应的sessionid，也就是ephemeralOwner
                if (createMode.isEphemeral()) {
                    s.setEphemeralOwner(request.sessionId);
                }
                // 拷贝parentRecord
                parentRecord = parentRecord.duplicate(request.hdr.getZxid());
                // 子节点数量+1
                parentRecord.childCount++;
                // 更新cversion
                parentRecord.stat.setCversion(newCversion);
                // 此时修改了父节点数据,所以父节点待更新
                // 将parentRecord添加到zk服务的outstandingChanges和outstandingChangesForPath中
                addChangeRecord(parentRecord);
                // 将新生成的ChangeRecord添加到zk服务的outstandingChanges和outstandingChangesForPath中
                addChangeRecord(new ChangeRecord(request.hdr.getZxid(), path, s,
                        0, listACL));
                break;
            case OpCode.delete:// 比如:  /com/simon/stu/zhangsan
                // 校验请求
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                // 将客户端请求添加到DeleteRequest中
                DeleteRequest deleteRequest = (DeleteRequest)record;
                // 反序列化出客户端的DeleteRequest
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, deleteRequest);
                // 获取要删除的路径
                path = deleteRequest.getPath();
                lastSlash = path.lastIndexOf('/');
                // 校验路径
                if (lastSlash == -1 || path.indexOf('\0') != -1
                        || zks.getZKDatabase().isSpecialPath(path)) {
                    throw new KeeperException.BadArgumentsException(path);
                }
                // 获取父路径:/com/simon/stu
                parentPath = path.substring(0, lastSlash);
                // 获取父路径对应的节点信息
                parentRecord = getRecordForPath(parentPath);
                // 获取路径对应的节点信息
                ChangeRecord nodeRecord = getRecordForPath(path);
                // 校验ACL
                checkACL(zks, parentRecord.acl, ZooDefs.Perms.DELETE,
                        request.authInfo);
                // 获取请求中版本号
                int version = deleteRequest.getVersion();
                // 校验请求中的版本号是否正确(类似于乐观锁)
                if (version != -1 && nodeRecord.stat.getVersion() != version) {
                    throw new KeeperException.BadVersionException(path);
                }
                // 要删除的path路径存在子节点,不允许删除
                if (nodeRecord.childCount > 0) {
                    throw new KeeperException.NotEmptyException(path);
                }
                // 创建txn
                request.txn = new DeleteTxn(path);
                // 更新父节点中子节点的数量
                parentRecord = parentRecord.duplicate(request.hdr.getZxid());
                parentRecord.childCount--;
                // 将变更后的节点添加到outstandingChanges和outstandingChangesForPath集合中
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(request.hdr.getZxid(), path,
                        null, -1, null));
                break;
            case OpCode.setData:
                // 1.检查会话
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                // 2.解析客户端的SetDataRequest
                SetDataRequest setDataRequest = (SetDataRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, setDataRequest);
                // 3.获取操作的路径,在客户端设置的就是serverPath
                path = setDataRequest.getPath();
                // 4.验证path路径是否有效
                validatePath(path, request.sessionId);
                // 5.获取path路径的ChangeRecord
                nodeRecord = getRecordForPath(path);
                // 6.校验客户端是否有ACL权限操作该节点
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.WRITE,
                        request.authInfo);
                // 7.获取客户端传入的version(类似行锁)
                version = setDataRequest.getVersion();
                // 8.获取服务端节点的实际version
                int currentVersion = nodeRecord.stat.getVersion();
                // 9.版本不匹配,抛出异常.这里可以看出客户端版本不能设置为-1
                if (version != -1 && version != currentVersion) {
                    throw new KeeperException.BadVersionException(path);
                }
                // 10.准备更新服务端节点的版本+1
                version = currentVersion + 1;
                // 11.设置请求的txn
                request.txn = new SetDataTxn(path, setDataRequest.getData(), version);
                // 12.复制nodeRecord并更新zxid
                nodeRecord = nodeRecord.duplicate(request.hdr.getZxid());
                // 13.更新赋值后nodeRecord的版本为最新的
                nodeRecord.stat.setVersion(version);
                // 14.路径节点发生变更,设置到outstandingChanges等队列中
                addChangeRecord(nodeRecord);
                break;
            case OpCode.setACL:
                // 检查session
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                // 反序列化出客户端的请求
                SetACLRequest setAclRequest = (SetACLRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, setAclRequest);
                // 获取操作路径
                path = setAclRequest.getPath();
                // 验证路径
                validatePath(path, request.sessionId);
                // 去除重复的ACL权限
                listACL = removeDuplicates(setAclRequest.getAcl());
                if (!fixupACL(request.authInfo, listACL)) {
                    throw new KeeperException.InvalidACLException(path);
                }
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.ADMIN,
                        request.authInfo);
                // 校验版本
                version = setAclRequest.getVersion();
                currentVersion = nodeRecord.stat.getAversion();
                if (version != -1 && version != currentVersion) {
                    throw new KeeperException.BadVersionException(path);
                }
                version = currentVersion + 1;
                request.txn = new SetACLTxn(path, listACL, version);
                nodeRecord = nodeRecord.duplicate(request.hdr.getZxid());
                // 更新aversion版本号
                nodeRecord.stat.setAversion(version);
                addChangeRecord(nodeRecord);
                break;
            case OpCode.createSession:
                request.request.rewind();
                // 读取session超时时间
                int to = request.request.getInt();
                // 设置事务记录
                request.txn = new CreateSessionTxn(to);
                request.request.rewind();
                // 记录sessionId和对应的超时时间
                // 并生成SessionImpl(如果不存在的话)
                zks.sessionTracker.addSession(request.sessionId, to);
                zks.setOwner(request.sessionId, request.getOwner());
                break;
            case OpCode.closeSession:
                // We don't want to do this check since the session expiration thread
                // queues up this operation without being the session owner.
                // this request is the last of the session so it should be ok
                //zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                // 获取sessionID关联的所有临时路径,内部是克隆一份
                HashSet<String> es = zks.getZKDatabase()
                        .getEphemerals(request.sessionId);
                // 从switch所处理的请求中可以看出,只有delete命令在生成的ChangeRecord中没有stat
                //
                synchronized (zks.outstandingChanges) {
                    for (ChangeRecord c : zks.outstandingChanges) {
                        if (c.stat == null) {
                            // Doing a delete
                            es.remove(c.path);
                        } else if (c.stat.getEphemeralOwner() == request.sessionId) {
                            es.add(c.path);
                        }
                    }
                    for (String path2Delete : es) {
                        addChangeRecord(new ChangeRecord(request.hdr.getZxid(),
                                path2Delete, null, 0, null));
                    }

                    zks.sessionTracker.setSessionClosing(request.sessionId);
                }

                LOG.info("Processed session termination for sessionid: 0x"
                        + Long.toHexString(request.sessionId));
                break;
            case OpCode.check:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                CheckVersionRequest checkVersionRequest = (CheckVersionRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, checkVersionRequest);
                path = checkVersionRequest.getPath();
                validatePath(path, request.sessionId);
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.READ,
                        request.authInfo);
                version = checkVersionRequest.getVersion();
                currentVersion = nodeRecord.stat.getVersion();
                if (version != -1 && version != currentVersion) {
                    throw new KeeperException.BadVersionException(path);
                }
                version = currentVersion + 1;
                request.txn = new CheckVersionTxn(path, version);
                break;
            default:
                LOG.error("Invalid OpCode: {} received by PrepRequestProcessor", type);
        }
    }

    private void validatePath(String path, long sessionId) throws BadArgumentsException {
        try {
            PathUtils.validatePath(path);
        } catch(IllegalArgumentException ie) {
            LOG.info("Invalid path " +  path + " with session 0x" + Long.toHexString(sessionId) +
                    ", reason: " + ie.getMessage());
            throw new BadArgumentsException(path);
        }
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param request
     */
    // 确定请求类型，并根据请求类型不同生成不同的请求对象
    @SuppressWarnings("unchecked")
    protected void pRequest(Request request) throws RequestProcessorException {
        // LOG.info("Prep>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = 0x" + Long.toHexString(request.sessionId));
        // 注:这里清空request中的hdr和txn
        request.hdr = null;
        request.txn = null;
        
        try {
            // 判断请求类型
            switch (request.type) {
                case OpCode.create:
                // 创建节点请求
                CreateRequest createRequest = new CreateRequest();
                // 封装请求交给pRequest2Txn()方法来处理
                pRequest2Txn(request.type, zks.getNextZxid(), request, createRequest, true);
                break;
            case OpCode.delete:
                // 创建删除节点请求
                DeleteRequest deleteRequest = new DeleteRequest();
                // 做一些前期的准备操作
                pRequest2Txn(request.type, zks.getNextZxid(), request, deleteRequest, true);
                break;
            case OpCode.setData:
                // 更新节点数据请求
                SetDataRequest setDataRequest = new SetDataRequest();                
                pRequest2Txn(request.type, zks.getNextZxid(), request, setDataRequest, true);
                break;
            case OpCode.setACL:
                // 创建SetACLRequest用于解析客户端请求中的SetACLRequest
                SetACLRequest setAclRequest = new SetACLRequest();                
                pRequest2Txn(request.type, zks.getNextZxid(), request, setAclRequest, true);
                break;
            case OpCode.check:
                CheckVersionRequest checkRequest = new CheckVersionRequest();              
                pRequest2Txn(request.type, zks.getNextZxid(), request, checkRequest, true);
                break;
            case OpCode.multi:
                // 解析请求中的MultiTransactionRecord
                MultiTransactionRecord multiRequest = new MultiTransactionRecord();
                try {
                    ByteBufferInputStream.byteBuffer2Record(request.request, multiRequest);
                } catch(IOException e) {
                    request.hdr =  new TxnHeader(request.sessionId, request.cxid, zks.getNextZxid(),
                            Time.currentWallTime(), OpCode.multi);
                    throw e;
                }
                List<Txn> txns = new ArrayList<Txn>();
                //Each op in a multi-op must have the same zxid!
                // 获取一个zxid,
                // 注意这里:OpCode.multi请求中包含的所有请求最终都是使用同一个zxid
                long zxid = zks.getNextZxid();
                KeeperException ke = null;

                //Store off current pending change records in case we need to rollback
                // 获取针对multiRequest中所操作路径的ChangeRecord
                // 这样是为了方便进行回滚操作,此时获取的这些ChangeRecord中的zxid肯定小于上面分配的zxid
                // 如果请求中操作了这些ChangeRecord那么等出现异常时,将pendingChanges中记录的操作前的记录回放即可
                HashMap<String, ChangeRecord> pendingChanges = getPendingChanges(multiRequest);

                int index = 0;
                for(Op op: multiRequest) {
                    // 将Op中的请求类型转换为对应的Request
                    Record subrequest = op.toRequestRecord() ;

                    /* If we've already failed one of the ops, don't bother
                     * trying the rest as we know it's going to fail and it
                     * would be confusing in the logfiles.
                     */
                    // 有一个请求出现了异常,后续所有请求都走这里
                    if (ke != null) {
                        // 封装异常信息
                        request.hdr.setType(OpCode.error);
                        request.txn = new ErrorTxn(Code.RUNTIMEINCONSISTENCY.intValue());
                    } 
                    
                    /* Prep the request and convert to a Txn */
                    // 预处理该请求
                    else {
                        try {
                            // 开始预处理请求
                            pRequest2Txn(op.getType(), zxid, request, subrequest, false);
                        } catch (KeeperException e) {
                            // 走到这里说明处理当前Op出现了异常
                            // 记录异常
                            ke = e;
                            // 设置请求头的类型以及事务
                            request.hdr.setType(OpCode.error);
                            request.txn = new ErrorTxn(e.code().intValue());
                            LOG.info("Got user-level KeeperException when processing "
                            		+ request.toString() + " aborting remaining multi ops."
                            		+ " Error Path:" + e.getPath()
                            		+ " Error:" + e.getMessage());

                            request.setException(e);

                            /* Rollback change records from failed multi-op */
                            // 出现异常,回滚刚刚所有的操作
                            rollbackPendingChanges(zxid, pendingChanges);
                        }
                    }

                    //FIXME: I don't want to have to serialize it here and then
                    //       immediately deserialize in next processor. But I'm 
                    //       not sure how else to get the txn stored into our list.

                    // 处理完当前请求则将请求中的Txn解析出来记录到txns集合中
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                    // 分别是CreateTxn/DeleteTxn/SetDataTxn/CheckVersionTxn
                    request.txn.serialize(boa, "request") ;
                    // 解析pRequest2Txn()方法执行后在request中设置的txn到baos
                    // 然后将txn转为ByteBuffer
                    // 最后将ByteBuffer以及当前请求的类型封装为一个Txn
                    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());

                    txns.add(new Txn(request.hdr.getType(), bb.array()));
                    index++;
                }
                // 创建事务头
                request.hdr = new TxnHeader(request.sessionId, request.cxid, zxid,
                        Time.currentWallTime(), request.type);
                // 创建MultiTxn
                request.txn = new MultiTxn(txns);
                
                break;

            //create/close session don't require request record
            // 创建/关闭会话不需要请求记录也就是xxxRequest
            case OpCode.createSession:
            case OpCode.closeSession:
                pRequest2Txn(request.type, zks.getNextZxid(), request, null, true);
                break;
 
            //All the rest don't need to create a Txn - just verify session
            // 以下请求都不需要创建Txn,只需要验证会话.所有以下请求都没有Txn和hdr
            case OpCode.sync:
            case OpCode.exists:
            case OpCode.getData:
            case OpCode.getACL:
            case OpCode.getChildren:
            case OpCode.getChildren2:
            case OpCode.ping:
            case OpCode.setWatches:
                zks.sessionTracker.checkSession(request.sessionId,
                        request.getOwner());
                break;
            default:
                LOG.warn("unknown type " + request.type);
                break;
            }
        } catch (KeeperException e) {
            if (request.hdr != null) {
                request.hdr.setType(OpCode.error);
                request.txn = new ErrorTxn(e.code().intValue());
            }
            LOG.info("Got user-level KeeperException when processing "
                    + request.toString()
                    + " Error Path:" + e.getPath()
                    + " Error:" + e.getMessage());
            request.setException(e);
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);

            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            if(bb != null){
                bb.rewind();
                while (bb.hasRemaining()) {
                    sb.append(Integer.toHexString(bb.get() & 0xff));
                }
            } else {
                sb.append("request buffer is null");
            }

            LOG.error("Dumping request buffer: 0x" + sb.toString());
            if (request.hdr != null) {
                request.hdr.setType(OpCode.error);
                request.txn = new ErrorTxn(Code.MARSHALLINGERROR.intValue());
            }
        }
        request.zxid = zks.getZxid();

        nextProcessor.processRequest(request);
    }

    // 去除重复的ACL
    private List<ACL> removeDuplicates(List<ACL> acl) {

        ArrayList<ACL> retval = new ArrayList<ACL>();
        Iterator<ACL> it = acl.iterator();
        while (it.hasNext()) {
            ACL a = it.next();
            if (retval.contains(a) == false) {
                retval.add(a);
            }
        }
        return retval;
    }


    /**
     * This method checks out the acl making sure it isn't null or empty,
     * it has valid schemes and ids, and expanding any relative ids that
     * depend on the requestor's authentication information.
     *
     * @param authInfo list of ACL IDs associated with the client connection 客户端对应的ServerCnxn所拥有的ACL
     * @param acl list of ACLs being assigned to the node (create or setACL operation) 分配给节点的ACL
     * @return
     */
    /**
     * 作用是在Create或setAcl命令给节点赋值权限时,解析当前客户端ServerCnxn所拥有的权限,然后获取对应权限模式的权限集合最后解析为ACL
     * 也就是最终访问这个节点所需要的权限
     * 如下命令如果注释掉那段没打开,在创建节点时将抛出KeeperException$InvalidACLException异常
     * ZooKeeper zk1 = new ZooKeeper("192.168.6.133", 50000, new Watcher() {
     *    @Override
     *    public void process(WatchedEvent watchedEvent) {
     * 		System.out.println("事件" + watchedEvent.getType());
     *    }
     * });
     * //zk1.addAuthInfo("digest", "admin:admin".getBytes());
     * zk1.create("/auth-test", "test".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);
     *
     * 如果打开上述代码,然后在加上下列代码,下列代码将抛出KeeperException$NoAuthException异常
     * ZooKeeper zk2 = new ZooKeeper("192.168.6.133", 50000, new Watcher() {
     *        @Override
     *    public void process(WatchedEvent watchedEvent) {
     *
     *    }
     * });
     * System.out.println(JSONUtil.toJsonStr(zk2.getData("/auth-test", false, null)));
     */
    private boolean fixupACL(List<Id> authInfo, List<ACL> acl) {
        // 读取配置zookeeper.skipACL跳过权限验证,默认true
        if (skipACL) {
            return true;
        }
        // acl不存在
        if (acl == null || acl.size() == 0) {
            return false;
        }
        // 遍历分配给节点的ACL
        Iterator<ACL> it = acl.iterator();
        LinkedList<ACL> toAdd = null;
        // 遍历分配给节点的ACL权限
        while (it.hasNext()) {
            // 获取分配的权限
            ACL a = it.next();
            // 获取授权模式和授权对象
            Id id = a.getId();
            // 1.授权模式是world并且授权对象是anyone
            // 也就是是一种开放的权限控制模式
            if (id.getScheme().equals("world") && id.getId().equals("anyone")) {
                // wide open
            // 2.授权模式是auth
                // 如果创建节点时分配了auth类型的权限模式,但是当前客户端却不存在这种模式的权限,则抛出异常认为是无效的权限模式
            } else if (id.getScheme().equals("auth")) {
                // This is the "auth" id, so we have to expand it to the
                // authenticated ids of the requestor
                // 这里删除了该ACL
                it.remove();
                if (toAdd == null) {
                    toAdd = new LinkedList<ACL>();
                }
                boolean authIdValid = false;
                // 获取客户端ServerCnxn所拥有的权限中isAuthenticated()方法为true的,然后解析为ACL
                // 也就是说访问当前节点需要具备的权限
                for (Id cid : authInfo) {
                    AuthenticationProvider ap =
                        ProviderRegistry.getProvider(cid.getScheme());
                    if (ap == null) {
                        LOG.error("Missing AuthenticationProvider for "
                                + cid.getScheme());
                    } else if (ap.isAuthenticated()) {
                        authIdValid = true;
                        toAdd.add(new ACL(a.getPerms(), cid));
                    }
                }
                if (!authIdValid) {
                    return false;
                }
                // 3.授权模式是ip或digest
            } else {
                AuthenticationProvider ap = ProviderRegistry.getProvider(id
                        .getScheme());
                if (ap == null) {
                    return false;
                }
                // 验证授权对象是否有效
                if (!ap.isValid(id.getId())) {
                    return false;
                }
            }
        }
        if (toAdd != null) {
            for (ACL a : toAdd) {
                acl.add(a);
            }
        }
        return acl.size() > 0;
    }

    // 处理请求，将请求添加到submittedRequests中,内部run()方法不断从该队列中获取请求进行处理
    public void processRequest(Request request) {
        // request.addRQRec(">prep="+zks.outstandingChanges.size());
        submittedRequests.add(request);
    }

    // 关闭当前处理器
    public void shutdown() {
        LOG.info("Shutting down");
        // 1.清空队列
        // 2.设置一个Request.requestOfDeath请求到队列中
        // 3.关闭整个链条中的下一个处理器
        submittedRequests.clear();
        submittedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }
}
