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

package org.apache.zookeeper.server.quorum.flexible;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Set;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;


/**
 * This class implements a validator for hierarchical quorums. With this
 * construction, zookeeper servers are split into disjoint groups, and 
 * each server has a weight. We obtain a quorum if we get more than half
 * of the total weight of a group for a majority of groups.
 * 
 * The configuration of quorums uses two parameters: group and weight. 
 * Groups are sets of ZooKeeper servers, and we set a group by passing
 * a colon-separated list of server ids. It is also necessary to assign
 * weights to server. Here is an example of a configuration that creates
 * three groups and assigns a weight of 1 to each server:
 * 
 *  group.1=1:2:3
 *  group.2=4:5:6
 *  group.3=7:8:9
 *  
 *  weight.1=1
 *  weight.2=1
 *  weight.3=1
 *  weight.4=1
 *  weight.5=1
 *  weight.6=1
 *  weight.7=1
 *  weight.8=1
 *  weight.9=1
 * 
 * Note that it is still necessary to define peers using the server keyword.
 */

public class QuorumHierarchical implements QuorumVerifier {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumHierarchical.class);
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
    HashMap<Long, Long> serverWeight;
    // group组成员
    // key是zk服务ID，value是组ID
    // group.1=1:2:3
    // group.2=4:5:6
    // group.3=7
    HashMap<Long, Long> serverGroup;
    // 记录组的权重信息
    // key是groupId，value是权重
    HashMap<Long, Long> groupWeight;
    //  录组的数量
    int numGroups;
   
    /**
     * This contructor requires the quorum configuration
     * to be declared in a separate file, and it takes the
     * file as an input parameter.
     */
    public QuorumHierarchical(String filename)
    throws ConfigException {
        this.serverWeight = new HashMap<Long, Long>();
        this.serverGroup = new HashMap<Long, Long>();
        this.groupWeight = new HashMap<Long, Long>();
        this.numGroups = 0;
        
        readConfigFile(filename);
    }
    
    /**
     * This constructor takes a set of properties. We use
     * it in the unit test for this feature.
     */
    
    public QuorumHierarchical(Properties qp)
    throws ConfigException {
        this.serverWeight = new HashMap<Long, Long>();
        this.serverGroup = new HashMap<Long, Long>();
        this.groupWeight = new HashMap<Long, Long>();
        this.numGroups = 0;
        
        parse(qp);
        
        LOG.info(serverWeight.size() + ", " + serverGroup.size() + ", " + groupWeight.size());
    }
    
   /**
    *  This contructor takes the two hash maps needed to enable 
    *  validating quorums. We use it with QuorumPeerConfig. That is,
    *  we declare weights and groups in the server configuration
    *  file along with the other parameters.
    * @param numGroups
    * @param serverWeight
    * @param serverGroup
    */
    public QuorumHierarchical(int numGroups,
            HashMap<Long, Long> serverWeight,
            HashMap<Long, Long> serverGroup)
    {

        this.serverWeight = serverWeight;
        this.serverGroup = serverGroup;
        this.groupWeight = new HashMap<Long, Long>();
        this.numGroups = numGroups;
        computeGroupWeight();   
    }
    
    
    /**
     * Returns the weight of a server.
     * 
     * @param id
     */
    public long getWeight(long id){
        return serverWeight.get(id);
    }
    
    /**
     * Reads a configration file. Called from the constructor
     * that takes a file as an input.
     */
    private void readConfigFile(String filename)
    throws ConfigException{
        File configFile = new File(filename);

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
    
            parse(cfg);
        } catch (IOException e) {
            throw new ConfigException("Error processing " + filename, e);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Error processing " + filename, e);
        }
        
    }
    
    
    /**
     * Parse properties if configuration given in a separate file.
     */
    private void parse(Properties quorumProp){
        for (Entry<Object, Object> entry : quorumProp.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString(); 
            if (key.startsWith("group")) {
                int dot = key.indexOf('.');
                long gid = Long.parseLong(key.substring(dot + 1));
                
                numGroups++;
                
                String parts[] = value.split(":");
                for(String s : parts){
                    long sid = Long.parseLong(s);
                    serverGroup.put(sid, gid);
                }
                    
                
            } else if(key.startsWith("weight")) {
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));
                serverWeight.put(sid, Long.parseLong(value));
            }
        }
        
        computeGroupWeight();
    }
    
    /**
     * This method pre-computes the weights of groups to speed up processing
     * when validating a given set. We compute the weights of groups in 
     * different places, so we have a separate method.
     */
    // 计算组的权重
    private void computeGroupWeight(){
        // 1.遍历所有的组，计算每个组的权重
        // key是zk服务ID，value是组ID
        for(Entry<Long, Long> entry : serverGroup.entrySet()){
            Long sid = entry.getKey();
            Long gid = entry.getValue();
            if(!groupWeight.containsKey(gid))
                groupWeight.put(gid, serverWeight.get(sid));
            else {
                long totalWeight = serverWeight.get(sid) + groupWeight.get(gid);
                groupWeight.put(gid, totalWeight);
            } 
        }    
        
        /*
         * Do not consider groups with weight zero
         */
        // 2.上面处理完所有组的权重后，过滤出权重为0的组，然后修改组的数量
        for(long weight: groupWeight.values()){
            LOG.debug("Group weight: " + weight);
            if(weight == ((long) 0)){
                numGroups--;
                LOG.debug("One zero-weight group: " + 1 + ", " + numGroups);
            }
        }
    }
    
    /**
     * Verifies if a given set is a quorum.
     */
    public boolean containsQuorum(Set<Long> set){
        HashMap<Long, Long> expansion = new HashMap<Long, Long>();
        
        /*
         * Adds up weights per group
         */
        if(set.size() == 0) return false;
        else LOG.debug("Set size: " + set.size());
        
        for(long sid : set){
            Long gid = serverGroup.get(sid);
            if(!expansion.containsKey(gid))
                expansion.put(gid, serverWeight.get(sid));
            else {
                long totalWeight = serverWeight.get(sid) + expansion.get(gid);
                expansion.put(gid, totalWeight);
            }
        }
  
        /*
         * Check if all groups have majority
         */
        int majGroupCounter = 0;
        for(Entry<Long, Long> entry : expansion.entrySet()) {
            Long gid = entry.getKey();
            LOG.debug("Group info: " + entry.getValue() + ", " + gid + ", " + groupWeight.get(gid));
            if(entry.getValue() > (groupWeight.get(gid) / 2) )
                majGroupCounter++;
        }
        
        LOG.debug("Majority group counter: " + majGroupCounter + ", " + numGroups); 
        if((majGroupCounter > (numGroups / 2))){
            LOG.debug("Positive set size: " + set.size());
            return true;
        }
        else {
            LOG.debug("Negative set size: " + set.size());
            return false;
        }
    }
}
