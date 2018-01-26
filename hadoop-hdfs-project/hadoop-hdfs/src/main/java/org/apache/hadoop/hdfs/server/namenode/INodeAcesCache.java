/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.hops.metadata.hdfs.entity.Ace;

import java.util.List;

class INodeAcesCache {
  
  private Cache<Integer, List<Ace>> inodesToAcls;
  private Cache<Ace.PrimaryKey, Ace> acePKeysToAces;
  
  INodeAcesCache(){
    inodesToAcls = Caffeine.newBuilder().build();
    acePKeysToAces = Caffeine.newBuilder().build();
  }
  
  List<Ace> getAcesByInode(int inodeId){
    return inodesToAcls.getIfPresent(inodeId);
  }
  
  Ace getAceByPK(int id, int inodeId){
    return acePKeysToAces.getIfPresent(new Ace.PrimaryKey(id, inodeId));
  }
  
  void invalidateAcesByInode(int inodeId){
    List<Ace> acesByInode = getAcesByInode(inodeId);
    if (acesByInode == null){
      return;
    }
    for (Ace ace : acesByInode) {
      acePKeysToAces.invalidate(ace.getPrimaryKey());
    }
    
    inodesToAcls.invalidate(inodeId);
  }
  
  void addAcesByInode(int inodeId, List<Ace> aces){
    if (aces == null){
      return;
    }
    for (Ace ace : aces) {
      acePKeysToAces.put(ace.getPrimaryKey(), ace);
    }
    inodesToAcls.put(inodeId, aces);
  }
  
  
  
}
