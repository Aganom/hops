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

import com.google.common.collect.Lists;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.AceDataAccess;
import io.hops.metadata.hdfs.entity.Ace;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class INodeAces {
  private static final Log LOG = LogFactory.getLog(INodeAces.class);
  
  private static boolean isInitialized;
  private static INodeAcesCache cache;
  
  public static void init(){
    if (isInitialized){
      LOG.warn("init() called while already initialized.");
      return;
    }
    cache = new INodeAcesCache();
    isInitialized = true;
  }
  
  /**
   * Get acl entries for inode with given inode id.
   *
   * @param inode
   * @return A list of acl entries. Empty list if no acl entries.
   * @throws IOException
   */
  static List<AclEntry> getInodeAcl(INode inode) throws IOException {
    checkInitialized();
    
    List<Ace> cached = cache.getAcesByInode(inode.id);
    if (cached == null ){
      List<Ace> fromDb = getFromDatabaseByInodeId(inode.id);
      
      cache.addAcesByInode(inode.id, fromDb);
      return convert(fromDb);
    }
    return convert(cached);
  }
  
  static List<AclEntry> getInodeAcesByIds(int inodeId, List<Integer> aceIds) throws IOException {
    List<Ace> entries = new ArrayList<>();
    
    for (Integer aceId : aceIds) {
      Ace toAdd = cache.getAceByPK(aceId, inodeId);
      if (toAdd == null){
        // If any is null simply get all from the database;
        Integer[] integers = aceIds.toArray(new Integer[aceIds.size()]);
        int[] ints = new int[integers.length];
        for (int i = 0; i < integers.length ; i++){
          ints[i] = integers[i];
        }
        List<Ace> pkBatched = getFromDatabasePKBatched(ints, inodeId);
        if (pkBatched == null){
          return null;
        }
        cache.addAcesByInode(inodeId,pkBatched);
        return convert(pkBatched);
      }
      entries.add(toAdd);
    }
    Collections.sort(entries, new Comparator<Ace>() {
      @Override
      public int compare(Ace o1, Ace o2) {
        return o1.getIndex() - o2.getIndex();
      }
    });
    return convert(entries);
  }
  
  /**
   * Add a list of aces to an inode.
   * @param inodeId
   * @param aclEntries
   * @return the ids of the created database rows.
   * @throws IOException
   */
  static List<Integer> addInodeAces(final int inodeId, List<AclEntry> aclEntries) throws IOException {
    checkInitialized();
    
    List<Integer> result = new ArrayList<>();
    for (int i = 0 ; i < aclEntries.size() ; i++){
      final int index = i;
      final AclEntry aclEntry = aclEntries.get(i);
      Ace added = (Ace) new LightWeightRequestHandler(HDFSOperationType.ADD_INODE_ACES) {
        @Override
        public Object performTask() throws IOException {
          Ace toAdd = newAce(inodeId, aclEntry, index);
          AceDataAccess ida = (AceDataAccess) HdfsStorageFactory.getDataAccess(AceDataAccess.class);
          return ida.addAce(toAdd);
        }
      }.handle();
      result.add(added.getId());
    }
    
    cache.invalidateAcesByInode(inodeId);
    //TODO invalidate other namenode's caches
    
    return result;
  }
  
  static void removeInodeAces(final int inodeId) throws IOException {
    checkInitialized();
    
    cache.invalidateAcesByInode(inodeId);
    //TODO invalidate other namenode's caches
    
    new LightWeightRequestHandler(HDFSOperationType.REMOVE_INODE_ACES) {
      @Override
      public Object performTask() throws IOException {
        AceDataAccess ida = (AceDataAccess) HdfsStorageFactory.getDataAccess(AceDataAccess.class);
        ida.removeAcesForInodeId(inodeId);
        return null;
      }
    }.handle();
  }
  
  private static List<Ace> getFromDatabaseByInodeId(final int inodeId) throws IOException {
    List<Ace> toReturn = (List<Ace>) new LightWeightRequestHandler(HDFSOperationType.GET_INODE_ACES_BY_INODE) {
      @Override
      public Object performTask() throws IOException {
        AceDataAccess ida = (AceDataAccess) HdfsStorageFactory
            .getDataAccess(AceDataAccess.class);
        return ida.getAcesByInodeId(inodeId);
      }
    }.handle();
    Collections.sort(toReturn, new Comparator<Ace>() {
      @Override
      public int compare(Ace o1, Ace o2) {
        return o1.getIndex() - o2.getIndex();
      }
    });
    return toReturn;
  }
  
  private static List<Ace> getFromDatabasePKBatched(final int[] ids, final int inodeId) throws IOException {
    return (List<Ace>) new LightWeightRequestHandler(HDFSOperationType.GET_INODE_ACES_BY_PK){
      
      @Override
      public Object performTask() throws IOException {
        AceDataAccess ida = (AceDataAccess) HdfsStorageFactory
            .getDataAccess(AceDataAccess.class);
        return ida.getAcesByPKBatched(ids, inodeId);
      }
    }.handle();
  }
  
  private static Ace newAce(int inodeId, AclEntry entry, int index){
    return new Ace(-1, inodeId, entry.getName(), convert(entry.getType()), entry.getScope().equals(AclEntryScope
        .DEFAULT), entry.getPermission().ordinal(), index);
  }
  
  private static Ace.AceType convert(AclEntryType type) {
    switch (type) {
      case USER:
        return Ace.AceType.USER;
      case GROUP:
        return Ace.AceType.GROUP;
      default:
        throw new RuntimeException("Only allowed types are USER, GROUP, not " + type.name());
    }
  }
  
  private static List<AclEntry> convert(List<Ace> aces){
    if (aces == null){
      return Lists.newArrayList();
    }
    ArrayList<AclEntry> aclEntries = new ArrayList<>();
    for (Ace ace : aces) {
      AclEntry entry = new AclEntry.Builder()
          .setType(convert(ace.getType())).setPermission(FsAction.values()[ace.getPermission()])
          .setScope(ace.isDefault()?AclEntryScope.DEFAULT:AclEntryScope.ACCESS)
          .setName(ace.getSubject()).build();
      aclEntries.add(entry);
    }
    return aclEntries;
  }
  
  private static AclEntryType convert(Ace.AceType from){
    switch (from){
      case USER: return AclEntryType.USER;
      case GROUP: return AclEntryType.GROUP;
      default:
        throw new RuntimeException("Only allowed types are USER, GROUP, not " + from.name());
    }
  }
  
  
  private static void checkInitialized(){
    if (!isInitialized){
      throw new RuntimeException("Trying to access uninitialized INodeAces-singleton.");
    }
  }
}
