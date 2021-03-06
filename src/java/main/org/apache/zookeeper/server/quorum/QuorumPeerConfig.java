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

package org.apache.zookeeper.server.quorum;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.auth.QuorumAuth;
import org.apache.zookeeper.server.quorum.flexible.QuorumHierarchical;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;

// zk配置类，记录所有的配置信息
@InterfaceAudience.Public
public class QuorumPeerConfig {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerConfig.class);
    // 配置文件中的：clientPortAddress和clientPort两个配置封装的一个对象
    // 3.3.0版本新增的配置，客户端连接
    protected InetSocketAddress clientPortAddress;
    // 如果没有特别指明，ZooKeeper将内存中的数据库和事务日志写入该位置
    protected String dataDir;
    // 该选项将事务日志写入dataLogDir而不是dataDir，从而将事物日志和数据分开
    protected String dataLogDir;
    // 默认3000
    // ZooKeeper使用的基本时间单位，以毫秒为单位，用于调节心跳和超时。如，最小会话超时时间将为2个tickTime
    protected int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;
    // 限制客户端的最大连接数
    protected int maxClientCnxns = 60;
    /** defaults to -1 if not set explicitly */
    // 最小会话超时时间，默认2个tickTime
    protected int minSessionTimeout = -1;
    /** defaults to -1 if not set explicitly */
    // 最大会话超时时间，默认20个tickTime
    protected int maxSessionTimeout = -1;
    //
    protected int initLimit;
    protected int syncLimit;
    protected int electionAlg = 3;
    protected int electionPort = 2182;
    // 当设置为true时，ZooKeeper服务器将监听所有可用IP地址的连接，
    // 而不仅仅是配置文件中的服务器列表中配置的地址。
    // 它影响处理ZAB协议和Fast Leader选举协议的连接。默认值为false
    protected boolean quorumListenOnAllIPs = false;
    // 记录zk服务，key是zk服务ID
    protected final HashMap<Long,QuorumServer> servers =
        new HashMap<Long, QuorumServer>();
    // 记录zk服务中observers类型，key是zk服务ID
    protected final HashMap<Long,QuorumServer> observers =
        new HashMap<Long, QuorumServer>();
    // 也就是myid文件里配置的值
    protected long serverId;
    // 记录权重，key是zk服务ID，value是权重
    // Weight可以调节一个组内单个节点的权重，默认每个节点的权重是1（如果配置是0不参与leader的选举）.每个组有一个法定数的概念，
    // 法定数等于组内所有节点的权重之和.此时判断一个组是否稳定，是要判断存活的节点权重之和是否大于该组法定数的权重
    // weight.1=3
    // weight.2=1
    // weight.3=1
    // weight.4=1
    // weight.5=1
    // weight.6=1
    // weight.7=1
    // Group1的法定数是：3+1+1=5，只要节点权重之和过半该组就是稳定的，当2,3,4,5,6挂掉，此时Group1和Group3是稳定状态，整个集群是稳定的
    protected HashMap<Long, Long> serverWeight = new HashMap<Long, Long>();
    // group组成员
    // key是zk服务sID，value是组ID
    // group.1=1:2:3
    // group.2=4:5:6
    // group.3=7
    protected HashMap<Long, Long> serverGroup = new HashMap<Long, Long>();
    // 记录组的数量
    protected int numGroups = 0;
    // 如果存在分组使用QuorumHierarchical，否则使用QuorumMaj
    protected QuorumVerifier quorumVerifier;
    // 快照保存的数量，默认3
    protected int snapRetainCount = 3;
    // PurgeTask触发的时间间隔(单位小时)，默认0
    protected int purgeInterval = 0;
    // Observer是否使用SyncRequestProcessor,默认true
    protected boolean syncEnabled = true;

    protected LearnerType peerType = LearnerType.PARTICIPANT;

    /** Configurations for the quorumpeer-to-quorumpeer sasl authentication */
    protected boolean quorumServerRequireSasl = false;
    protected boolean quorumLearnerRequireSasl = false;
    protected boolean quorumEnableSasl = false;
    protected String quorumServicePrincipal = QuorumAuth.QUORUM_KERBEROS_SERVICE_PRINCIPAL_DEFAULT_VALUE;
    protected String quorumLearnerLoginContext = QuorumAuth.QUORUM_LEARNER_SASL_LOGIN_CONTEXT_DFAULT_VALUE;
    protected String quorumServerLoginContext = QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT_DFAULT_VALUE;
    protected int quorumCnxnThreadsSize;

    /**
     * Minimum snapshot retain count.
     * @see org.apache.zookeeper.server.PurgeTxnLog#purge(File, File, int)
     */
    private final int MIN_SNAP_RETAIN_COUNT = 3;

    @SuppressWarnings("serial")
    public static class ConfigException extends Exception {
        public ConfigException(String msg) {
            super(msg);
        }
        public ConfigException(String msg, Exception e) {
            super(msg, e);
        }
    }

    private static String[] splitWithLeadingHostname(String s)
            throws ConfigException
    {
        /* Does it start with an IPv6 literal? */
        if (s.startsWith("[")) {
            int i = s.indexOf("]:");
            if (i < 0) {
                throw new ConfigException(s + " starts with '[' but has no matching ']:'");
            }

            String[] sa = s.substring(i + 2).split(":");
            String[] nsa = new String[sa.length + 1];
            nsa[0] = s.substring(1, i);
            System.arraycopy(sa, 0, nsa, 1, sa.length);

            return nsa;
        } else {
            return s.split(":");
        }
    }

    /**
     * Parse a ZooKeeper configuration file
     * @param path the patch of the configuration file
     * @throws ConfigException error processing configuration
     */
    // 解析ZooKeeper配置文件
    public void parse(String path) throws ConfigException {
        File configFile = new File(path);

        LOG.info("Reading configuration from: " + configFile);

        try {
            if (!configFile.exists()) {
                throw new IllegalArgumentException(configFile.toString()
                        + " file is missing");
            }

            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                cfg.load(in);
            } finally {
                in.close();
            }

            parseProperties(cfg);
        } catch (IOException e) {
            throw new ConfigException("Error processing " + path, e);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Error processing " + path, e);
        }
    }

    /**
     * Parse config from a Properties.
     * @param zkProp Properties to parse from.
     * @throws IOException
     * @throws ConfigException
     */
    // 加载并解析配置文件
    public void parseProperties(Properties zkProp)
    throws IOException, ConfigException {
        int clientPort = 0;
        String clientPortAddress = null;
        for (Entry<Object, Object> entry : zkProp.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            if (key.equals("dataDir")) {
                dataDir = value;
            } else if (key.equals("dataLogDir")) {
                dataLogDir = value;
            } else if (key.equals("clientPort")) {
                clientPort = Integer.parseInt(value);
            } else if (key.equals("clientPortAddress")) {
                clientPortAddress = value.trim();
            } else if (key.equals("tickTime")) {
                tickTime = Integer.parseInt(value);
            } else if (key.equals("maxClientCnxns")) {
                maxClientCnxns = Integer.parseInt(value);
            } else if (key.equals("minSessionTimeout")) {
                minSessionTimeout = Integer.parseInt(value);
            } else if (key.equals("maxSessionTimeout")) {
                maxSessionTimeout = Integer.parseInt(value);
            } else if (key.equals("initLimit")) {
                initLimit = Integer.parseInt(value);
            } else if (key.equals("syncLimit")) {
                syncLimit = Integer.parseInt(value);
            } else if (key.equals("electionAlg")) {
                electionAlg = Integer.parseInt(value);
            } else if (key.equals("quorumListenOnAllIPs")) {
                quorumListenOnAllIPs = Boolean.parseBoolean(value);
            } else if (key.equals("peerType")) {
                if (value.toLowerCase().equals("observer")) {
                    peerType = LearnerType.OBSERVER;
                } else if (value.toLowerCase().equals("participant")) {
                    peerType = LearnerType.PARTICIPANT;
                } else
                {
                    throw new ConfigException("Unrecognised peertype: " + value);
                }
            } else if (key.equals( "syncEnabled" )) {
                syncEnabled = Boolean.parseBoolean(value);
            } else if (key.equals("autopurge.snapRetainCount")) {
                snapRetainCount = Integer.parseInt(value);
            } else if (key.equals("autopurge.purgeInterval")) {
                purgeInterval = Integer.parseInt(value);
            // 解析server.配置，如配置文件中内容如下,最后的角色信息可以不配置,不配置就是participant
            // server.1=192.168.6.130:2888:3888:observer
            // server.2=192.168.6.131:2888:3888:participant
            // server.3=192.168.6.132:2888:3888:participant
            } else if (key.startsWith("server.")) {
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));
                // parts解析后内容为：[192.168.6.130,2888,3888,participant]
                String parts[] = splitWithLeadingHostname(value);
                if ((parts.length != 2) && (parts.length != 3) && (parts.length !=4)) {
                    LOG.error(value
                       + " does not have the form host:port or host:port:port " +
                       " or host:port:port:type");
                }
                LearnerType type = null;
                // 获取服务器ip:192.168.6.130
                String hostname = parts[0];
                // 获取服务器第一个端口:2888(用来连接leader)
                Integer port = Integer.parseInt(parts[1]);
                // 获取服务器第二个端口:3888(用来选举leader)
                Integer electionPort = null;
                if (parts.length > 2){
                	electionPort=Integer.parseInt(parts[2]);
                }
                // 解析zk服务在集群中的角色
                if (parts.length > 3){
                    if (parts[3].toLowerCase().equals("observer")) {
                        type = LearnerType.OBSERVER;
                    } else if (parts[3].toLowerCase().equals("participant")) {
                        type = LearnerType.PARTICIPANT;
                    } else {
                        throw new ConfigException("Unrecognised peertype: " + value);
                    }
                }
                if (type == LearnerType.OBSERVER){
                    observers.put(Long.valueOf(sid), new QuorumServer(sid, hostname, port, electionPort, type));
                } else {
                    servers.put(Long.valueOf(sid), new QuorumServer(sid, hostname, port, electionPort, type));
                }
            // 解析组,比如配置如下(上面的配置应该修改为7台zk服务而不是三台)：
            // group.1=1:2:3
            // group.2=4:5:6
            // group.3=7
            } else if (key.startsWith("group")) {
                int dot = key.indexOf('.');
                // 获取组ID
                long gid = Long.parseLong(key.substring(dot + 1));
                // 组+1
                numGroups++;
                // 解析组中的成员并翻到对应的组中
                String parts[] = value.split(":");
                for(String s : parts){
                    long sid = Long.parseLong(s);
                    if(serverGroup.containsKey(sid))
                        throw new ConfigException("Server " + sid + "is in multiple groups");
                    else
                        serverGroup.put(sid, gid);
                }
            // 获取权重并记录权重，配置如下
            //  weight.1=3
            //  weight.2=1
            //  weight.3=1
            //  weight.4=1
            //  weight.5=1
            //  weight.6=1
            //  weight.7=1
            } else if(key.startsWith("weight")) {
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));
                serverWeight.put(sid, Long.parseLong(value));
            } else if (key.equals(QuorumAuth.QUORUM_SASL_AUTH_ENABLED)) {
                quorumEnableSasl = Boolean.parseBoolean(value);
            } else if (key.equals(QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED)) {
                quorumServerRequireSasl = Boolean.parseBoolean(value);
            } else if (key.equals(QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED)) {
                quorumLearnerRequireSasl = Boolean.parseBoolean(value);
            } else if (key.equals(QuorumAuth.QUORUM_LEARNER_SASL_LOGIN_CONTEXT)) {
                quorumLearnerLoginContext = value;
            } else if (key.equals(QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT)) {
                quorumServerLoginContext = value;
            } else if (key.equals(QuorumAuth.QUORUM_KERBEROS_SERVICE_PRINCIPAL)) {
                quorumServicePrincipal = value;
            } else if (key.equals("quorum.cnxn.threads.size")) {
                quorumCnxnThreadsSize = Integer.parseInt(value);
            } else {
                System.setProperty("zookeeper." + key, value);
            }
        }
        if (!quorumEnableSasl && quorumServerRequireSasl) {
            throw new IllegalArgumentException(
                    QuorumAuth.QUORUM_SASL_AUTH_ENABLED
                    + " is disabled, so cannot enable "
                    + QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED);
        }
        if (!quorumEnableSasl && quorumLearnerRequireSasl) {
            throw new IllegalArgumentException(
                    QuorumAuth.QUORUM_SASL_AUTH_ENABLED
                    + " is disabled, so cannot enable "
                    + QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED);
        }
        // If quorumpeer learner is not auth enabled then self won't be able to
        // join quorum. So this condition is ensuring that the quorumpeer learner
        // is also auth enabled while enabling quorum server require sasl.
        if (!quorumLearnerRequireSasl && quorumServerRequireSasl) {
            throw new IllegalArgumentException(
                    QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED
                    + " is disabled, so cannot enable "
                    + QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED);
        }
        // Reset to MIN_SNAP_RETAIN_COUNT if invalid (less than 3)
        // PurgeTxnLog.purge(File, File, int) will not allow to purge less
        // than 3.
        if (snapRetainCount < MIN_SNAP_RETAIN_COUNT) {
            LOG.warn("Invalid autopurge.snapRetainCount: " + snapRetainCount
                    + ". Defaulting to " + MIN_SNAP_RETAIN_COUNT);
            snapRetainCount = MIN_SNAP_RETAIN_COUNT;
        }

        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir is not set");
        }
        if (dataLogDir == null) {
            dataLogDir = dataDir;
        }
        if (clientPort == 0) {
            throw new IllegalArgumentException("clientPort is not set");
        }
        // 未配置clientPortAddress，则以当前服务器IP为clientPortAddress
        if (clientPortAddress != null) {
            this.clientPortAddress = new InetSocketAddress(
                    InetAddress.getByName(clientPortAddress), clientPort);
        } else {
            this.clientPortAddress = new InetSocketAddress(clientPort);
        }

        if (tickTime == 0) {
            throw new IllegalArgumentException("tickTime is not set");
        }
        if (minSessionTimeout > maxSessionTimeout) {
            throw new IllegalArgumentException(
                    "minSessionTimeout must not be larger than maxSessionTimeout");
        }
        if (servers.size() == 0) {
            if (observers.size() > 0) {
                throw new IllegalArgumentException("Observers w/o participants is an invalid configuration");
            }
            // Not a quorum configuration so return immediately - not an error
            // case (for b/w compatibility), server will default to standalone
            // mode.
            return;
        } else if (servers.size() == 1) {
            if (observers.size() > 0) {
                throw new IllegalArgumentException("Observers w/o quorum is an invalid configuration");
            }

            // HBase currently adds a single server line to the config, for
            // b/w compatibility reasons we need to keep this here.
            LOG.error("Invalid configuration, only one server specified (ignoring)");
            servers.clear();
        } else if (servers.size() > 1) {
            if (servers.size() == 2) {
                LOG.warn("No server failure will be tolerated. " +
                    "You need at least 3 servers.");
            } else if (servers.size() % 2 == 0) {
                LOG.warn("Non-optimial configuration, consider an odd number of servers.");
            }
            if (initLimit == 0) {
                throw new IllegalArgumentException("initLimit is not set");
            }
            if (syncLimit == 0) {
                throw new IllegalArgumentException("syncLimit is not set");
            }
            /*
             * If using FLE, then every server requires a separate election
             * port.
             */
            if (electionAlg != 0) {
                for (QuorumServer s : servers.values()) {
                    if (s.electionAddr == null)
                        throw new IllegalArgumentException(
                                "Missing election port for server: " + s.id);
                }
            }

            /*
             * Default of quorum config is majority
             */
            if(serverGroup.size() > 0){
                if(servers.size() != serverGroup.size())
                    throw new ConfigException("Every server must be in exactly one group");
                /*
                 * The deafult weight of a server is 1
                 */
                for(QuorumServer s : servers.values()){
                    if(!serverWeight.containsKey(s.id))
                        serverWeight.put(s.id, (long) 1);
                }

                /*
                 * Set the quorumVerifier to be QuorumHierarchical
                 */
                quorumVerifier = new QuorumHierarchical(numGroups,
                        serverWeight, serverGroup);
            } else {
                /*
                 * The default QuorumVerifier is QuorumMaj
                 */

                LOG.info("Defaulting to majority quorums");
                quorumVerifier = new QuorumMaj(servers.size());
            }

            // Now add observers to servers, once the quorums have been
            // figured out
            servers.putAll(observers);

            File myIdFile = new File(dataDir, "myid");
            if (!myIdFile.exists()) {
                throw new IllegalArgumentException(myIdFile.toString()
                        + " file is missing");
            }
            BufferedReader br = new BufferedReader(new FileReader(myIdFile));
            String myIdString;
            try {
                myIdString = br.readLine();
            } finally {
                br.close();
            }
            try {
                serverId = Long.parseLong(myIdString);
                MDC.put("myid", myIdString);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("serverid " + myIdString
                        + " is not a number");
            }
            
            // Warn about inconsistent peer type
            LearnerType roleByServersList = observers.containsKey(serverId) ? LearnerType.OBSERVER
                    : LearnerType.PARTICIPANT;
            if (roleByServersList != peerType) {
                LOG.warn("Peer type from servers list (" + roleByServersList
                        + ") doesn't match peerType (" + peerType
                        + "). Defaulting to servers list.");
    
                peerType = roleByServersList;
            }
        }
    }

    public InetSocketAddress getClientPortAddress() { return clientPortAddress; }
    public String getDataDir() { return dataDir; }
    public String getDataLogDir() { return dataLogDir; }
    public int getTickTime() { return tickTime; }
    public int getMaxClientCnxns() { return maxClientCnxns; }
    public int getMinSessionTimeout() { return minSessionTimeout; }
    public int getMaxSessionTimeout() { return maxSessionTimeout; }

    public int getInitLimit() { return initLimit; }
    public int getSyncLimit() { return syncLimit; }
    public int getElectionAlg() { return electionAlg; }
    public int getElectionPort() { return electionPort; }
    
    public int getSnapRetainCount() {
        return snapRetainCount;
    }

    public int getPurgeInterval() {
        return purgeInterval;
    }
    
    public boolean getSyncEnabled() {
        return syncEnabled;
    }

    public QuorumVerifier getQuorumVerifier() {
        return quorumVerifier;
    }

    public Map<Long,QuorumServer> getServers() {
        return Collections.unmodifiableMap(servers);
    }

    public long getServerId() { return serverId; }

    public boolean isDistributed() { return servers.size() > 1; }

    public LearnerType getPeerType() {
        return peerType;
    }

    public Boolean getQuorumListenOnAllIPs() {
        return quorumListenOnAllIPs;
    }
}
