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
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.Ace;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.AclException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class INodeAclHelper {
  
  /**
   *
   * @param inode
   * @return
   */
  static boolean hasOwnAcl(INode inode) {
    //Aces are always populated in order: ace1, ace2 and "hasMoreAces",
    //so checking the first is sufficient
    return inode.hasAce1();
  }
  
  /**
   *
   * @param inode
   * @return
   * @throws TransactionContextException
   * @throws StorageException
   */
  static AclFeature getAclFeature(INode inode) throws TransactionContextException, StorageException, AclException {
    List<Ace> result = getAces(inode);
    if (result == null){
      return null;
    }
    Collections.sort(result, Ace.Order.ByIndexAscending);
    return new AclFeature(convert(result));
  }
  
  /**
   *
   * @param inode
   * @param aclFeature
   * @throws TransactionContextException
   * @throws StorageException
   */
  static void addAclFeature(INode inode, AclFeature aclFeature)
      throws TransactionContextException, StorageException, AclException {
    List<AclEntry> entries = aclFeature.getEntries();
    for (AclEntry entry : entries) {
      if (entry.getName() == null || entry.getName().isEmpty()){
        throw new RuntimeException();
      }
    }
    int inodeId = inode.getId();
    if (!entries.isEmpty()){
      inode.setHasAce1NoPersistence(true);
      EntityManager.update(convert(entries.get(0), inodeId, 0));
    }
    if (entries.size() > 1){
      inode.setHasAce2NoPersistence(true);
      EntityManager.update(convert(entries.get(1), inodeId, 1));
    }
    if (entries.size() > 2) {
      for (int i = 2 ; i < entries.size() ; i++){
        EntityManager.update(convert(entries.get(i), inodeId, i));
      }
      inode.setHasMoreAcesNoPersistence(true);
    }
    inode.save();
  }
  
  public static void removeAclFeature(INode inode) throws TransactionContextException, StorageException {
    assert inode.hasAce1();
    Collection<Ace> aces = getOwnAces(inode);
    if (aces == null){
      return;
    }
    
    for (Ace ace : aces) {
      EntityManager.remove(ace);
    }
    inode.setHasAce1(false);
    inode.setHasAce2(false);
    inode.setHasMoreAces(false);
    inode.save();
  }
  
  private static List<Ace> getAces(INode inode) throws TransactionContextException, StorageException {
    Collection<Ace> aces;
    
    if (inode.hasOwnAcl()){
      aces = getOwnAces(inode);
    } else {
      //Check for inherited aces
      aces = getInheritedDefaultAcesAsAccess(inode.getParent());
    }
    
    if (aces == null){
      return null;
    }
    
    return Lists.newArrayList(aces);
  }
  
  private static Collection<Ace> getOwnAces(INode inode) throws TransactionContextException, StorageException {
    assert inode.hasOwnAcl();
    Collection<Ace> aces;
    if (inode.hasMoreAces()){
      //Get by pruned index scan all the aces
      aces = getAllAces(inode);
    } else {
      //Get by primary key up to two aces
      aces = getCachedAces(inode);
    }
    return aces;
  }
  
  private static Collection<Ace> getInheritedDefaultAcesAsAccess(INode inode)
      throws TransactionContextException, StorageException {
    if (inode == null){
      return null;
    }
    
    if(inode.hasOwnAcl()){
      Collection<Ace> ownAces = getOwnAces(inode);
      Collection<Ace> defaultAces = new ArrayList<>();
      
      for (Ace ownAce : ownAces) {
        if (ownAce.isDefault()){
          defaultAces.add(ownAce);
        }
      }
      
      if (!defaultAces.isEmpty()){
        //We found aces to inherit, return them converted
        Collection<Ace> convertedToAccess = new ArrayList<>();
        for (Ace defaultAce : defaultAces) {
          Ace access = defaultAce.copy();
          access.setIsDefault(false);
          convertedToAccess.add(access);
        }
        return convertedToAccess;
      }
    }
    
    //No default aces on this inode, keep traversing
    return getInheritedDefaultAcesAsAccess(inode.getParent());
  }
  
  private static Collection<Ace> getCachedAces(INode inode) throws TransactionContextException, StorageException {
    assert inode.hasAce1();
    
    int[] ids;
    if (inode.hasAce2()){
      ids = new int[]{0,1};
    } else {
      ids = new int[]{0};
    }
    return EntityManager.findList(Ace.Finder.ByInodeIdAndAceIds, inode.getId(), ids);
  }
  
  private static Collection<Ace> getAllAces(INode inode) throws TransactionContextException, StorageException {
    return EntityManager.findList(Ace.Finder.ByInodeId, inode.getId());
  }
  
  private static Ace convert(AclEntry entry, int inodeId, int index) throws AclException {
    return new Ace(inodeId,
        index,
        entry.getName(),
        convert(entry.getType()),
        entry.getScope().equals(AclEntryScope.DEFAULT),
        entry.getPermission().ordinal());
  }
  
  private static Ace.AceType convert(AclEntryType type) throws AclException {
    switch(type){
      case USER:
        return Ace.AceType.USER;
      case GROUP:
        return Ace.AceType.GROUP;
      default:
        throw new AclException("Unexpected acl entry type " + type.toString() + ", should be USER or GROUP.");
    }
  }
  
  private static AclEntryType convert(Ace.AceType type) throws AclException {
    switch(type){
      case USER:
        return AclEntryType.USER;
      case GROUP:
        return AclEntryType.GROUP;
      default:
        throw new AclException("Unexpected ace type " + type.toString() + ", should be USER or GROUP.");
      
    }
  }
  
  private static List<AclEntry> convert(List<Ace> aces) throws AclException {
    List<AclEntry> result = new ArrayList<>();
    for (Ace ace : aces) {
      result.add(new AclEntry.Builder()
          .setScope(ace.isDefault()?AclEntryScope.DEFAULT:AclEntryScope.ACCESS)
          .setName(ace.getSubject())
          .setPermission(FsAction.values()[ace.getPermission()])
          .setType(convert(ace.getType())).build());
    }
    return result;
  }
}
