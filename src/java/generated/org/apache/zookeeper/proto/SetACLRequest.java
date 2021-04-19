// File generated by hadoop record compiler. Do not edit.
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

package org.apache.zookeeper.proto;

import org.apache.jute.*;
import org.apache.yetus.audience.InterfaceAudience;
@InterfaceAudience.Public
public class SetACLRequest implements Record {
  // 操作的路径
  private String path;
  // 要更新为的权限列表
  private java.util.List<org.apache.zookeeper.data.ACL> acl;
  // 旧的版本
  private int version;
  public SetACLRequest() {
  }
  public SetACLRequest(
        String path,
        java.util.List<org.apache.zookeeper.data.ACL> acl,
        int version) {
    this.path=path;
    this.acl=acl;
    this.version=version;
  }
  public String getPath() {
    return path;
  }
  public void setPath(String m_) {
    path=m_;
  }
  public java.util.List<org.apache.zookeeper.data.ACL> getAcl() {
    return acl;
  }
  public void setAcl(java.util.List<org.apache.zookeeper.data.ACL> m_) {
    acl=m_;
  }
  public int getVersion() {
    return version;
  }
  public void setVersion(int m_) {
    version=m_;
  }
  public void serialize(OutputArchive a_, String tag) throws java.io.IOException {
    a_.startRecord(this,tag);
    a_.writeString(path,"path");
    {
      a_.startVector(acl,"acl");
      if (acl!= null) {          int len1 = acl.size();
          for(int vidx1 = 0; vidx1<len1; vidx1++) {
            org.apache.zookeeper.data.ACL e1 = (org.apache.zookeeper.data.ACL) acl.get(vidx1);
    a_.writeRecord(e1,"e1");
          }
      }
      a_.endVector(acl,"acl");
    }
    a_.writeInt(version,"version");
    a_.endRecord(this,tag);
  }
  public void deserialize(InputArchive a_, String tag) throws java.io.IOException {
    a_.startRecord(tag);
    path=a_.readString("path");
    {
      Index vidx1 = a_.startVector("acl");
      if (vidx1!= null) {          acl=new java.util.ArrayList<org.apache.zookeeper.data.ACL>();
          for (; !vidx1.done(); vidx1.incr()) {
    org.apache.zookeeper.data.ACL e1;
    e1= new org.apache.zookeeper.data.ACL();
    a_.readRecord(e1,"e1");
            acl.add(e1);
          }
      }
    a_.endVector("acl");
    }
    version=a_.readInt("version");
    a_.endRecord(tag);
}
  public String toString() {
    try {
      java.io.ByteArrayOutputStream s =
        new java.io.ByteArrayOutputStream();
      CsvOutputArchive a_ = 
        new CsvOutputArchive(s);
      a_.startRecord(this,"");
    a_.writeString(path,"path");
    {
      a_.startVector(acl,"acl");
      if (acl!= null) {          int len1 = acl.size();
          for(int vidx1 = 0; vidx1<len1; vidx1++) {
            org.apache.zookeeper.data.ACL e1 = (org.apache.zookeeper.data.ACL) acl.get(vidx1);
    a_.writeRecord(e1,"e1");
          }
      }
      a_.endVector(acl,"acl");
    }
    a_.writeInt(version,"version");
      a_.endRecord(this,"");
      return new String(s.toByteArray(), "UTF-8");
    } catch (Throwable ex) {
      ex.printStackTrace();
    }
    return "ERROR";
  }
  public void write(java.io.DataOutput out) throws java.io.IOException {
    BinaryOutputArchive archive = new BinaryOutputArchive(out);
    serialize(archive, "");
  }
  public void readFields(java.io.DataInput in) throws java.io.IOException {
    BinaryInputArchive archive = new BinaryInputArchive(in);
    deserialize(archive, "");
  }
  public int compareTo (Object peer_) throws ClassCastException {
    throw new UnsupportedOperationException("comparing SetACLRequest is unimplemented");
  }
  public boolean equals(Object peer_) {
    if (!(peer_ instanceof SetACLRequest)) {
      return false;
    }
    if (peer_ == this) {
      return true;
    }
    SetACLRequest peer = (SetACLRequest) peer_;
    boolean ret = false;
    ret = path.equals(peer.path);
    if (!ret) return ret;
    ret = acl.equals(peer.acl);
    if (!ret) return ret;
    ret = (version==peer.version);
    if (!ret) return ret;
     return ret;
  }
  public int hashCode() {
    int result = 17;
    int ret;
    ret = path.hashCode();
    result = 37*result + ret;
    ret = acl.hashCode();
    result = 37*result + ret;
    ret = (int)version;
    result = 37*result + ret;
    return result;
  }
  public static String signature() {
    return "LSetACLRequest(s[LACL(iLId(ss))]i)";
  }
}
