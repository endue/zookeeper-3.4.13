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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.jute.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * This is the structure that represents a request moving through a chain of
 * RequestProcessors. There are various pieces of information that is tacked
 * onto the request as it is processed.
 */
// 一个请求在处理链中的结构,在处理过程中会被添加上各种各样的信息
public class Request {
    private static final Logger LOG = LoggerFactory.getLogger(Request.class);

    public final static Request requestOfDeath = new Request(null, 0, 0, 0,
            null, null);

    /**
     * @param cnxn
     * @param sessionId
     * @param xid
     * @param type
     * @param bb
     */
    public Request(ServerCnxn cnxn, long sessionId, int xid, int type,
            ByteBuffer bb, List<Id> authInfo) {
        this.cnxn = cnxn;
        this.sessionId = sessionId;
        this.cxid = xid;
        this.type = type;
        this.request = bb;
        this.authInfo = authInfo;
    }

    public Request(long sessionId, int xid, int type, TxnHeader hdr, Record txn, long zxid) {
        this.sessionId = sessionId;
        this.cxid = xid;
        this.type = type;
        this.hdr = hdr;
        this.txn = txn;
        this.zxid = zxid;
        this.request = null;
        this.cnxn = null;
        this.authInfo = null;
    }
    // 客户端的sessionID
    public final long sessionId;
    // 客户端发送该请求时分配的xid
    public final int cxid;
    /**
     *  客户端发送的Request类型,{@link org.apache.zookeeper.ZooDefs.OpCode }
     */
    public final int type;
    // 如果是createSession请求,记录了session超时时间
    // 客户端发送请求时携带的请求体(针对不同操作携带不同数据)
    // 就是org.apache.zookeeper.ClientCnxn.Packet中的bb属性
    public final ByteBuffer request;
    // 当前请求所属客户端在服务端对应的ServerCnxn
    public final ServerCnxn cnxn;
    // 请求头
    public TxnHeader hdr;
    // 如果是createSession请求,记录了CreateSessionTxn
    // 感觉可以理解为请求体
    public Record txn;

    public long zxid = -1;
    // 当前请求所属客户端在服务端对应的ServerCnxn中记录的authInfo
    public final List<Id> authInfo;

    public final long createTime = Time.currentElapsedTime();
    /**
     *  当前请求的所有者类型
     * 参考{@link ZooKeeperServer#processPacket(org.apache.zookeeper.server.ServerCnxn, java.nio.ByteBuffer)}
     */
    private Object owner;
    
    private KeeperException e;

    public Object getOwner() {
        return owner;
    }
    
    public void setOwner(Object owner) {
        this.owner = owner;
    }

    /**
     * is the packet type a valid packet in zookeeper
     * 
     * @param type
     *                the type of the packet
     * @return true if a valid packet, false if not
     */
    static boolean isValid(int type) {
        // make sure this is always synchronized with Zoodefs!!
        switch (type) {
        case OpCode.notification:
            return false;
        case OpCode.create:
        case OpCode.delete:
        case OpCode.createSession:
        case OpCode.exists:
        case OpCode.getData:
        case OpCode.check:
        case OpCode.multi:
        case OpCode.setData:
        case OpCode.sync:
        case OpCode.getACL:
        case OpCode.setACL:
        case OpCode.getChildren:
        case OpCode.getChildren2:
        case OpCode.ping:
        case OpCode.closeSession:
        case OpCode.setWatches:
            return true;
        default:
            return false;
        }
    }

    static boolean isQuorum(int type) {
        switch (type) {
        case OpCode.exists:
        case OpCode.getACL:
        case OpCode.getChildren:
        case OpCode.getChildren2:
        case OpCode.getData:
            return false;
        case OpCode.error:
        case OpCode.closeSession:
        case OpCode.create:
        case OpCode.createSession:
        case OpCode.delete:
        case OpCode.setACL:
        case OpCode.setData:
        case OpCode.check:
        case OpCode.multi:
            return true;
        default:
            return false;
        }
    }
    
    static String op2String(int op) {
        switch (op) {
        case OpCode.notification:
            return "notification";
        case OpCode.create:
            return "create";
        case OpCode.setWatches:
            return "setWatches";
        case OpCode.delete:
            return "delete";
        case OpCode.exists:
            return "exists";
        case OpCode.getData:
            return "getData";
        case OpCode.check:
            return "check";
        case OpCode.multi:
            return "multi";
        case OpCode.setData:
            return "setData";
        case OpCode.sync:
              return "sync:";
        case OpCode.getACL:
            return "getACL";
        case OpCode.setACL:
            return "setACL";
        case OpCode.getChildren:
            return "getChildren";
        case OpCode.getChildren2:
            return "getChildren2";
        case OpCode.ping:
            return "ping";
        case OpCode.createSession:
            return "createSession";
        case OpCode.closeSession:
            return "closeSession";
        case OpCode.error:
            return "error";
        default:
            return "unknown " + op;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("sessionid:0x").append(Long.toHexString(sessionId))
            .append(" type:").append(op2String(type))
            .append(" cxid:0x").append(Long.toHexString(cxid))
            .append(" zxid:0x").append(Long.toHexString(hdr == null ?
                    -2 : hdr.getZxid()))
            .append(" txntype:").append(hdr == null ?
                    "unknown" : "" + hdr.getType());

        // best effort to print the path assoc with this request
        String path = "n/a";
        if (type != OpCode.createSession
                && type != OpCode.setWatches
                && type != OpCode.closeSession
                && request != null
                && request.remaining() >= 4)
        {
            try {
                // make sure we don't mess with request itself
                ByteBuffer rbuf = request.asReadOnlyBuffer();
                rbuf.clear();
                int pathLen = rbuf.getInt();
                // sanity check
                if (pathLen >= 0
                        && pathLen < 4096
                        && rbuf.remaining() >= pathLen)
                {
                    byte b[] = new byte[pathLen];
                    rbuf.get(b);
                    path = new String(b);
                }
            } catch (Exception e) {
                // ignore - can't find the path, will output "n/a" instead
            }
        }
        sb.append(" reqpath:").append(path);

        return sb.toString();
    }

    public void setException(KeeperException e) {
        this.e = e;
    }
	
    public KeeperException getException() {
        return e;
    }
}
