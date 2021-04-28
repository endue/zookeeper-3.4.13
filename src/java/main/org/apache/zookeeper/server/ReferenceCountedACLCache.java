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

import org.apache.jute.Index;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 该类记录了三个重点:
 * 1.acl集合对应的long值
 * 2.long值对应的acl集合的关系
 * 3.记录了acl对应的long值的引用次数
 *
 * 各节点上的信息，只保存了acl的整形值，这个整形值就是对acl缓存的引用值，而真正的acl集合
 * 信息缓存在ReferenceCountedACLCache类中，对于datatree整个树形结构来说节省了大量的内存
 */
public class ReferenceCountedACLCache {
    private static final Logger LOG = LoggerFactory.getLogger(ReferenceCountedACLCache.class);
    // key是long,value是acl列表
    final Map<Long, List<ACL>> longKeyMap =
            new HashMap<Long, List<ACL>>();
    // key是acl列表,value是long
    final Map<List<ACL>, Long> aclKeyMap =
            new HashMap<List<ACL>, Long>();
    // key是acl对应的long值,value是记录的引用次数
    final Map<Long, AtomicLongWithEquals> referenceCounter =
            new HashMap<Long, AtomicLongWithEquals>();
    // acls列表为空时,对应的long值
    private static final long OPEN_UNSAFE_ACL_ID = -1L;

    /**
     * these are the number of acls that we have in the datatree
     */
    // acls列表对应的long值每次都是根据aclIndex+1进行计算
    long aclIndex = 0;

    /**
     * converts the list of acls to a long.
     * Increments the reference counter for this ACL.
     * @param acls
     * @return a long that map to the acls
     */
    // acl集合列表转long
    public synchronized Long convertAcls(List<ACL> acls) {
        if (acls == null)
            return OPEN_UNSAFE_ACL_ID;

        // get the value from the map
        // 查询是否有该acl集合对应的long值
        Long ret = aclKeyMap.get(acls);
        // 没有
        if (ret == null) {
            // 根据aclIndex递增生成一个ret
            ret = incrementIndex();
            // 保存起来
            longKeyMap.put(ret, acls);
            aclKeyMap.put(acls, ret);
        }
        // 修改计数器+1
        addUsage(ret);

        return ret;
    }

    /**
     * converts a long to a list of acls.
     *
     * @param longVal
     * @return a list of ACLs that map to the long
     */
    // long类型转acl列表
    public synchronized List<ACL> convertLong(Long longVal) {
        if (longVal == null)
            return null;
        // -1对应的acl列表集合
        if (longVal == OPEN_UNSAFE_ACL_ID)
            return ZooDefs.Ids.OPEN_ACL_UNSAFE;
        // 转换long为acl列表集合
        List<ACL> acls = longKeyMap.get(longVal);
        if (acls == null) {
            LOG.error("ERROR: ACL not available for long " + longVal);
            throw new RuntimeException("Failed to fetch acls for " + longVal);
        }
        return acls;
    }

    private long incrementIndex() {
        return ++aclIndex;
    }

    // 反序列化
    public synchronized void deserialize(InputArchive ia) throws IOException {
        clear();
        int i = ia.readInt("map");
        while (i > 0) {
            Long val = ia.readLong("long");
            if (aclIndex < val) {
                aclIndex = val;
            }
            List<ACL> aclList = new ArrayList<ACL>();
            Index j = ia.startVector("acls");
            if (j == null) {
                throw new RuntimeException("Incorrent format of InputArchive when deserialize DataTree - missing acls");
            }
            while (!j.done()) {
                ACL acl = new ACL();
                acl.deserialize(ia, "acl");
                aclList.add(acl);
                j.incr();
            }
            longKeyMap.put(val, aclList);
            aclKeyMap.put(aclList, val);
            referenceCounter.put(val, new AtomicLongWithEquals(0));
            i--;
        }
    }
    // 序列化
    public synchronized void serialize(OutputArchive oa) throws IOException {
        oa.writeInt(longKeyMap.size(), "map");
        Set<Map.Entry<Long, List<ACL>>> set = longKeyMap.entrySet();
        for (Map.Entry<Long, List<ACL>> val : set) {
            oa.writeLong(val.getKey(), "long");
            List<ACL> aclList = val.getValue();
            oa.startVector(aclList, "acls");
            for (ACL acl : aclList) {
                acl.serialize(oa, "acl");
            }
            oa.endVector(aclList, "acls");
        }
    }

    public int size() {
        return aclKeyMap.size();
    }

    private void clear() {
        aclKeyMap.clear();
        longKeyMap.clear();
        referenceCounter.clear();
    }

    public synchronized void addUsage(Long acl) {
        if (acl == OPEN_UNSAFE_ACL_ID) {
            return;
        }

        if (!longKeyMap.containsKey(acl)) {
            LOG.info("Ignoring acl " + acl + " as it does not exist in the cache");
            return;
        }
        // 获取acl对应long值对应的AtomicLong计数器
        AtomicLong count = referenceCounter.get(acl);
        // 计数器+1,表示有多少个节点引用了该acl列表集合
        if (count == null) {
            referenceCounter.put(acl, new AtomicLongWithEquals(1));
        } else {
            count.incrementAndGet();
        }
    }
    // 删除acl集合
    public synchronized void removeUsage(Long acl) {
        if (acl == OPEN_UNSAFE_ACL_ID) {
            return;
        }

        if (!longKeyMap.containsKey(acl)) {
            LOG.info("Ignoring acl " + acl + " as it does not exist in the cache");
            return;
        }
        // 递减引用次数
        long newCount = referenceCounter.get(acl).decrementAndGet();
        // 如果没有了引用次数,则删除相关记录
        if (newCount <= 0) {
            referenceCounter.remove(acl);
            aclKeyMap.remove(longKeyMap.get(acl));
            longKeyMap.remove(acl);
        }
    }
    // 清除未使用的acl集合对应的long值
    public synchronized void purgeUnused() {
        Iterator<Map.Entry<Long, AtomicLongWithEquals>> refCountIter = referenceCounter.entrySet().iterator();
        while (refCountIter.hasNext()) {
            Map.Entry<Long, AtomicLongWithEquals> entry = refCountIter.next();
            if (entry.getValue().get() <= 0) {
                Long acl = entry.getKey();
                aclKeyMap.remove(longKeyMap.get(acl));
                longKeyMap.remove(acl);
                refCountIter.remove();
            }
        }
    }

    private static class AtomicLongWithEquals extends AtomicLong {

        private static final long serialVersionUID = 3355155896813725462L;

        public AtomicLongWithEquals(long i) {
            super(i);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            return equals((AtomicLongWithEquals) o);
        }

        public boolean equals(AtomicLongWithEquals that) {
            return get() == that.get();
        }

        @Override
        public int hashCode() {
            return 31 * Long.valueOf(get()).hashCode();
        }
    }
}
