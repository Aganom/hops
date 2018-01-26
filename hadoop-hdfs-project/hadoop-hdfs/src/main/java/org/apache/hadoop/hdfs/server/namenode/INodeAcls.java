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

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.AceDataAccess;
import io.hops.metadata.hdfs.entity.Ace;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class INodeAcls {
  
  private static boolean isInitialized;
  private static INodeAcesCache cache;
  
  public static void init(){
    if (isInitialized){
      return;
    }
    cache = new INodeAcesCache();
    isInitialized = true;
  }
  
  static List<AclEntry> getInodeAcl(int inodeId) throws IOException {
    if (!isInitialized){
      return null;
    }
  
    List<Ace> cached = cache.getAcesByInode(inodeId);
    if (cached == null ){
      List<Ace> fromDb = getFromDatabaseByInodeId(inodeId);
      
  
      cache.addAcesByInode(inodeId, fromDb);
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
  static List<Integer> addInodeAcl(final int inodeId, List<AclEntry> aclEntries) throws IOException {
    if (!isInitialized){
      return null;
    }
    
    List<Integer> result = new ArrayList<>();
    for (int i = 0 ; i < aclEntries.size() ; i++){
      final int index = i;
      final AclEntry aclEntry = aclEntries.get(i);
      Ace added = (Ace) new LightWeightRequestHandler(HDFSOperationType.ADD_INODE_ACE) {
        @Override
        public Object performTask() throws IOException {
          Ace toAdd = new Ace(-1, inodeId, aclEntry.getName(),fromAclEntryType(aclEntry.getType()), aclEntry
              .getScope().equals(AclEntryScope.DEFAULT), aclEntry.getPermission().ordinal(), index);
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
  
  static void removeInodeAcl(final int inodeId) throws IOException {
    if (!isInitialized){
      return;
    }
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
  
  private static List<AclEntry> convert(List<Ace> aces){
    if (aces == null){
      return null;
    }
    ArrayList<AclEntry> aclEntries = new ArrayList<>();
    for (Ace ace : aces) {
      AclEntry entry = new AclEntry.Builder()
          .setType(fromAceType(ace.getType())).setPermission(FsAction.values()[ace.getPermission()])
          .setScope(ace.isDefault()?AclEntryScope.DEFAULT:AclEntryScope.ACCESS)
          .setName(ace.getSubject()).build();
      aclEntries.add(entry);
    }
    return aclEntries;
  }
  
  private static List<Ace> getFromDatabaseByInodeId(final int inodeId) throws IOException {
    List<Ace> toReturn = (List<Ace>) new LightWeightRequestHandler(HDFSOperationType.GET_INODE_ACES) {
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
    return (List<Ace>) new LightWeightRequestHandler(HDFSOperationType.GET_INODE_ACES){
  
      @Override
      public Object performTask() throws IOException {
        AceDataAccess ida = (AceDataAccess) HdfsStorageFactory
            .getDataAccess(AceDataAccess.class);
        return ida.getAcesByPKBatched(ids, inodeId);
      }
    }.handle();
  }
  
  private static Ace.AceType fromAclEntryType(AclEntryType from){
    switch (from){
      case USER: return Ace.AceType.USER;
      case GROUP: return Ace.AceType.GROUP;
      case MASK: return Ace.AceType.MASK;
      case OTHER: return Ace.AceType.OTHER;
      default:
        throw new RuntimeException("Only allowed types are USER, GROUP, MASK and OTHER, not " + from.name());
    }
  }
  
  private static AclEntryType fromAceType(Ace.AceType from){
    switch (from){
      case USER: return AclEntryType.USER;
      case GROUP: return AclEntryType.GROUP;
      case MASK: return AclEntryType.MASK;
      case OTHER: return AclEntryType.OTHER;
      default:
        throw new RuntimeException("Only allowed types are USER, GROUP, MASK and OTHER, not " + from.name());
    }
  }
}
