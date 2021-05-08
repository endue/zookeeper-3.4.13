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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.jute.Record;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetACLRequest;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.proto.SyncResponse;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.TxnHeader;

import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.CheckResult;
import org.apache.zookeeper.OpResult.CreateResult;
import org.apache.zookeeper.OpResult.DeleteResult;
import org.apache.zookeeper.OpResult.SetDataResult;
import org.apache.zookeeper.OpResult.ErrorResult;

/**
 * This Request processor actually applies any transaction associated with a
 * request and services any queries. It is always at the end of a
 * RequestProcessor chain (hence the name), so it does not have a nextProcessor
 * member.
 *
 * This RequestProcessor counts on ZooKeeperServer to populate the
 * outstandingRequests member of ZooKeeperServer.
 */
// 没有nextProcessor，作为处理器链条中的最后一个
// FinalRequestProcessor并没有实现ZooKeeperCriticalThread只是实现了RequestProcessor，
// 所以它不能作为一个线程启动并且也只有RequestProcessor接口中的两个方法
public class FinalRequestProcessor implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(FinalRequestProcessor.class);
    // zk服务器
    ZooKeeperServer zks;

    public FinalRequestProcessor(ZooKeeperServer zks) {
        this.zks = zks;
    }

    // 处理request
    public void processRequest(Request request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        // request.addRQRec(">final");
        long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
        if (request.type == OpCode.ping) {
            traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logRequest(LOG, traceMask, 'E', request, "");
        }
        // 记录处理结果
        ProcessTxnResult rc = null;
        // outstandingChanges队列中记录了被修改但是还未刷入内存目录树的数据
        // 锁住zk服务器中的outstandingChanges队列
        synchronized (zks.outstandingChanges) {
            // while循环不断从outstandingChanges队列获取 <= 当前请求zxid的被修改路径对应的ChangeRecord
            // Processor是一个个的请求处理的并且对应每个请求都会分配一个递增的zxid,所以当请求当前请求时,那么<= 当前请求的zxid
            // 一定是被处理过了,删除记录
            while (!zks.outstandingChanges.isEmpty()
                    && zks.outstandingChanges.get(0).zxid <= request.zxid) {
                ChangeRecord cr = zks.outstandingChanges.remove(0);
                if (cr.zxid < request.zxid) {
                    LOG.warn("Zxid outstanding "
                            + cr.zxid
                            + " is less than current " + request.zxid);
                }
                if (zks.outstandingChangesForPath.get(cr.path) == cr) {
                    zks.outstandingChangesForPath.remove(cr.path);
                }
            }
            // hdr不为空,说明是事务请求,然后将请求中的操作刷入内存目录树
            if (request.hdr != null) {
               TxnHeader hdr = request.hdr;
               Record txn = request.txn;
               // 处理事务请求写入内存文件目录树
                // 创建节点/删除节点...操作
                // 其中也包含了处理createSession和closeSession
               rc = zks.processTxn(hdr, txn);
            }
            // do not add non quorum packets to the queue.
            /**
             * 校验是否为事务性请求，如果是则创建Proposal到committedLog队列中
             * committedLog队列中记录了最近的500个事务请求
             * 这里的用处是集群模式中,参考{@link org.apache.zookeeper.server.quorum.LearnerHandler#run}
             */
            if (Request.isQuorum(request.type)) {
                zks.getZKDatabase().addCommittedProposal(request);
            }
        }
        // 关闭session请求
        if (request.hdr != null && request.hdr.getType() == OpCode.closeSession) {
            ServerCnxnFactory scxn = zks.getServerCnxnFactory();
            // this might be possible since
            // we might just be playing diffs from the leader
            if (scxn != null && request.cnxn == null) {
                // calling this if we have the cnxn results in the client's
                // close session response being lost - we've already closed
                // the session/socket here before we can send the closeSession
                // in the switch block below
                scxn.closeSession(request.sessionId);
                return;
            }
        }

        /*------准备发送响应给客户端--------*/

        // 集群模式时,leader发送给learner的请求,learner处理的时候cnxn为null
        if (request.cnxn == null) {
            return;
        }
        // 获取对应发送当前请求的客户端的ServerCnxn
        ServerCnxn cnxn = request.cnxn;
        // 返回操作类型
        String lastOp = "NA";
        // 请求处理完毕,递减一个
        zks.decInProcess();
        // 返回操作的异常
        Code err = Code.OK;
        // 返回操作的数据
        Record rsp = null;
        boolean closeSession = false;
        try {
            if (request.hdr != null && request.hdr.getType() == OpCode.error) {
                throw KeeperException.create(KeeperException.Code.get((
                        (ErrorTxn) request.txn).getErr()));
            }

            KeeperException ke = request.getException();
            if (ke != null && request.type != OpCode.multi) {
                throw ke;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("{}",request);
            }
            switch (request.type) {
                // 处理ping请求
            case OpCode.ping: {
                zks.serverStats().updateLatency(request.createTime);

                lastOp = "PING";
                cnxn.updateStatsForResponse(request.cxid, request.zxid, lastOp,
                        request.createTime, Time.currentElapsedTime());
                // 返回响应,响应头默认为-2
                cnxn.sendResponse(new ReplyHeader(-2,
                        zks.getZKDatabase().getDataTreeLastProcessedZxid(), 0), null, "response");
                return;
            }
            case OpCode.createSession: {
                zks.serverStats().updateLatency(request.createTime);

                lastOp = "SESS";
                // 更新ServerCnxn中的一些统计信息
                cnxn.updateStatsForResponse(request.cxid, request.zxid, lastOp,
                        request.createTime, Time.currentElapsedTime());
                // session初始化完毕,创建响应返回给客户端
                zks.finishSessionInit(request.cnxn, true);
                return;
            }
            // 多个操作命令组成的事务请求
            case OpCode.multi: {
                lastOp = "MULT";
                // 创建响应
                rsp = new MultiResponse() ;
                // 遍历处理后的结果
                for (ProcessTxnResult subTxnResult : rc.multiResult) {

                    OpResult subResult ;
                    // 转换结果为对应的数据
                    switch (subTxnResult.type) {
                        case OpCode.check:
                            subResult = new CheckResult();
                            break;
                        case OpCode.create:
                            subResult = new CreateResult(subTxnResult.path);
                            break;
                        case OpCode.delete:
                            subResult = new DeleteResult();
                            break;
                        case OpCode.setData:
                            subResult = new SetDataResult(subTxnResult.stat);
                            break;
                        case OpCode.error:
                            subResult = new ErrorResult(subTxnResult.err) ;
                            break;
                        default:
                            throw new IOException("Invalid type of op");
                    }
                    // 记录结果到rsp中返回给客户端
                    ((MultiResponse)rsp).add(subResult);
                }

                break;
            }
            case OpCode.create: {
                lastOp = "CREA";
                rsp = new CreateResponse(rc.path);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.delete: {
                lastOp = "DELE";
                err = Code.get(rc.err);
                break;
            }
            case OpCode.setData: {
                lastOp = "SETD";
                rsp = new SetDataResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.setACL: {
                lastOp = "SETA";
                rsp = new SetACLResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.closeSession: {
                lastOp = "CLOS";
                closeSession = true;
                err = Code.get(rc.err);
                break;
            }
            // 根据SyncRequest中的操作路径生成SyncResponse返回给客户端
            case OpCode.sync: {
                lastOp = "SYNC";
                SyncRequest syncRequest = new SyncRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        syncRequest);
                rsp = new SyncResponse(syncRequest.getPath());
                break;
            }
            case OpCode.check: {
                lastOp = "CHEC";
                rsp = new SetDataResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.exists: {
                lastOp = "EXIS";
                // TODO we need to figure out the security requirement for this!
                // 序列化Request中的ExistsRequest
                ExistsRequest existsRequest = new ExistsRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        existsRequest);
                // 获取ExistsRequest中的路径path
                String path = existsRequest.getPath();
                if (path.indexOf('\0') != -1) {
                    throw new KeeperException.BadArgumentsException();
                }
                // 获取path路径对应的节点信息,然后内部还会注册针对path路径的事件
                // 也就是根据请求中的watch属性
                Stat stat = zks.getZKDatabase().statNode(path, existsRequest
                        .getWatch() ? cnxn : null);
                // 将节点信息添加到ExistsResponse中
                rsp = new ExistsResponse(stat);
                break;
            }
            case OpCode.getData: {
                lastOp = "GETD";
                // 解析客户端发送的GetDataRequest
                GetDataRequest getDataRequest = new GetDataRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        getDataRequest);
                // 读取路径下的节点信息
                DataNode n = zks.getZKDatabase().getNode(getDataRequest.getPath());
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                // 校验是否拥有权限
                PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                        ZooDefs.Perms.READ,
                        request.authInfo);
                // 获取节点的状态信息以及节点上存储的数据然后返回给客户端
                Stat stat = new Stat();
                byte b[] = zks.getZKDatabase().getData(getDataRequest.getPath(), stat,
                        getDataRequest.getWatch() ? cnxn : null);
                rsp = new GetDataResponse(b, stat);
                break;
            }
            /**
             * 客户端在重新连接时会注册其本地保存的Watcher
             * {@link org.apache.zookeeper.ClientCnxn.SendThread#primeConnection()}
             * 发送的数据包为:new Packet(h, new ReplyHeader(), sw, null, null)
             * 其中sw为:SetWatches(setWatchesLastZxid, dataWatchesBatch, existWatchesBatch, childWatchesBatch);
             */
                case OpCode.setWatches: {
                lastOp = "SETW";
                // 读取请求中的所有事件监听
                SetWatches setWatches = new SetWatches();
                // XXX We really should NOT need this!!!!
                request.request.rewind();
                ByteBufferInputStream.byteBuffer2Record(request.request, setWatches);
                long relativeZxid = setWatches.getRelativeZxid();
                zks.getZKDatabase().setWatches(relativeZxid, 
                        setWatches.getDataWatches(), 
                        setWatches.getExistWatches(),
                        setWatches.getChildWatches(), cnxn);
                break;
            }
            case OpCode.getACL: {
                lastOp = "GETA";
                GetACLRequest getACLRequest = new GetACLRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        getACLRequest);
                Stat stat = new Stat();
                List<ACL> acl = 
                    zks.getZKDatabase().getACL(getACLRequest.getPath(), stat);
                rsp = new GetACLResponse(acl, stat);
                break;
            }
            case OpCode.getChildren: {
                lastOp = "GETC";
                GetChildrenRequest getChildrenRequest = new GetChildrenRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        getChildrenRequest);
                DataNode n = zks.getZKDatabase().getNode(getChildrenRequest.getPath());
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                        ZooDefs.Perms.READ,
                        request.authInfo);
                List<String> children = zks.getZKDatabase().getChildren(
                        getChildrenRequest.getPath(), null, getChildrenRequest
                                .getWatch() ? cnxn : null);
                rsp = new GetChildrenResponse(children);
                break;
            }
            case OpCode.getChildren2: {
                lastOp = "GETC";
                GetChildren2Request getChildren2Request = new GetChildren2Request();
                ByteBufferInputStream.byteBuffer2Record(request.request,
                        getChildren2Request);
                Stat stat = new Stat();
                DataNode n = zks.getZKDatabase().getNode(getChildren2Request.getPath());
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                        ZooDefs.Perms.READ,
                        request.authInfo);
                List<String> children = zks.getZKDatabase().getChildren(
                        getChildren2Request.getPath(), stat, getChildren2Request
                                .getWatch() ? cnxn : null);
                rsp = new GetChildren2Response(children, stat);
                break;
            }
            }
        } catch (SessionMovedException e) {
            // session moved is a connection level error, we need to tear
            // down the connection otw ZOOKEEPER-710 might happen
            // ie client on slow follower starts to renew session, fails
            // before this completes, then tries the fast follower (leader)
            // and is successful, however the initial renew is then 
            // successfully fwd/processed by the leader and as a result
            // the client and leader disagree on where the client is most
            // recently attached (and therefore invalid SESSION MOVED generated)
            cnxn.sendCloseSession();
            return;
        } catch (KeeperException e) {
            err = e.code();
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);
            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            bb.rewind();
            while (bb.hasRemaining()) {
                sb.append(Integer.toHexString(bb.get() & 0xff));
            }
            LOG.error("Dumping request buffer: 0x" + sb.toString());
            err = Code.MARSHALLINGERROR;
        }
        // 获取最后处理的zxid
        long lastZxid = zks.getZKDatabase().getDataTreeLastProcessedZxid();
        // 设置响应头
        // 设置客户端发送请求时分配的xid
        // 设置服务端处理当前请求分配的zxid
        // 设置以及处理该请求对应的异常
        ReplyHeader hdr =
            new ReplyHeader(request.cxid, lastZxid, err.intValue());

        zks.serverStats().updateLatency(request.createTime);
        cnxn.updateStatsForResponse(request.cxid, lastZxid, lastOp,
                    request.createTime, Time.currentElapsedTime());

        try {
            // 发送响应
            cnxn.sendResponse(hdr, rsp, "response");
            if (closeSession) {
                cnxn.sendCloseSession();
            }
        } catch (IOException e) {
            LOG.error("FIXMSG",e);
        }
    }

    public void shutdown() {
        // we are the final link in the chain
        LOG.info("shutdown of request processor complete");
    }

}
