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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

import static io.hops.metadata.hdfs.entity.Ace.NON_EXISTING_ACE_ID;
import static org.apache.hadoop.hdfs.server.namenode.INodeDirectory.ROOT_ID;

/**
 * AclStorage contains utility methods that define how ACL data is stored in the
 * namespace.
 *
 * If an inode has an ACL, then the ACL bit is set in the inode's
 * {@link FsPermission} and the inode also contains an {@link AclFeature}.  For
 * the access ACL, the owner and other entries are identical to the owner and
 * other bits stored in FsPermission, so we reuse those.  The access mask entry
 * is stored into the group permission bits of FsPermission.  This is consistent
 * with other file systems' implementations of ACLs and eliminates the need for
 * special handling in various parts of the codebase.  For example, if a user
 * calls chmod to change group permission bits on a file with an ACL, then the
 * expected behavior is to change the ACL's mask entry.  By saving the mask entry
 * into the group permission bits, chmod continues to work correctly without
 * special handling.  All remaining access entries (named users and named groups)
 * are stored as explicit {@link AclEntry} instances in a list inside the
 * AclFeature.  Additionally, all default entries are stored in the AclFeature.
 *
 * The methods in this class encapsulate these rules for reading or writing the
 * ACL entries to the appropriate location.
 *
 * The methods in this class assume that input ACL entry lists have already been
 * validated and sorted according to the rules enforced by
 * {@link AclTransformation}.
 */
@InterfaceAudience.Private
final class AclStorage {

  /**
   * Reads the existing extended ACL entries of an inode.  This method returns
   * only the extended ACL entries stored in the AclFeature.  If the inode does
   * not have an ACL, then this method returns an empty list.  This method
   * supports querying by snapshot ID.
   *
   * @param inode INode to read
   * @return List<AclEntry> containing extended inode ACL entries
   */
  public static List<AclEntry> readINodeAcl(INode inode) throws IOException {
    AclFeature aclFeature = getAclFeature(inode);
    return aclFeature == null ? ImmutableList.<AclEntry>of() : aclFeature.getEntries();
  }

  /**
   * Completely removes the ACL from an inode.
   *
   * @param inode INode to update
   * @throws QuotaExceededException if quota limit is exceeded
   */
  public static void removeINodeAcl(INode inode)
      throws IOException {
    AclFeature f = getAclFeature(inode);
    if (f == null) {
      return;
    }
    
    removeAclFeature(inode);
  }

  /**
   * Updates an inode with a new ACL.  This method takes a full logical ACL and
   * stores the entries to the inode's {@link FsPermission} and
   * {@link AclFeature}.
   *
   * @param inode INode to update
   * @param newAcl List<AclEntry> containing new ACL entries
   * @throws AclException if the ACL is invalid for the given inode
   * @throws QuotaExceededException if quota limit is exceeded
   */
  public static void updateINodeAcl(INode inode, List<AclEntry> newAcl)
      throws IOException {
    
    //assert newAcl.size() >= 3;
    FsPermission perm = inode.getFsPermission();
    // This is an extended ACL.  Split entries into access vs. default.
    ScopedAclEntries scoped = new ScopedAclEntries(newAcl);
    List<AclEntry> accessEntries = scoped.getAccessEntries();
    List<AclEntry> defaultEntries = scoped.getDefaultEntries();

    // Only directories may have a default ACL.
    if (!defaultEntries.isEmpty() && !inode.isDirectory()) {
      throw new AclException(
        "Invalid ACL: only directories may have a default ACL.");
    }

    // Attach entries to the feature.
    if (getAclFeature(inode) != null) {
      removeAclFeature(inode);
    }
    
    addAclFeature(inode, createInodeAcl(accessEntries, defaultEntries));
    
    inode.setPermission(perm);
  }

  /**
   * There is no reason to instantiate this class.
   */
  private AclStorage() {
  }

  /**
   * Creates an AclFeature from the given ACL entries.
   *
   * @param accessEntries List<AclEntry> access ACL entries
   * @param defaultEntries List<AclEntry> default ACL entries
   * @return AclFeature containing the required ACL entries
   */
  private static AclFeature createInodeAcl(List<AclEntry> accessEntries,
      List<AclEntry> defaultEntries) {

    List<AclEntry> featureEntries = Lists.newArrayListWithCapacity(
      accessEntries.size() + defaultEntries.size());
    
    featureEntries.addAll(accessEntries);

    // Add all default entries to the feature.
    featureEntries.addAll(defaultEntries);
    
    Collections.sort(featureEntries, AclTransformation.ACL_ENTRY_COMPARATOR);
    return new AclFeature(Collections.unmodifiableList(featureEntries));
  }
  

  /**
   * Creates the new FsPermission for an inode that is receiving an extended
   * ACL, based on its access ACL entries.  For a correctly sorted ACL, the
   * first entry is the owner and the last 2 entries are the mask and other
   * entries respectively.  Also preserve sticky bit and toggle ACL bit on.
   *
   * @param accessEntries List<AclEntry> access ACL entries
   * @param existingPerm FsPermission existing permissions
   * @return FsPermission new permissions
   */
  private static FsPermission createFsPermissionForExtendedAcl(
      List<AclEntry> accessEntries, FsPermission existingPerm) {
    return existingPerm;
//    return new FsPermission(accessEntries.get(0).getPermission(),
//      accessEntries.get(accessEntries.size() - 2).getPermission(),
//      accessEntries.get(accessEntries.size() - 1).getPermission(),
//      existingPerm.getStickyBit());
  }
  
  //ACEs are stored associated with their inodes.
  //An ace can be referred to by an id.
  //All aces are stored in a table, but the first two aces for any inode are cached as references inside the inode.
  
  private static AclFeature getAclFeature(INode inode) throws IOException {
    if (inode.getId() == ROOT_ID){
      return null;
    }
    List<AclEntry> result;
    if (hasOwnAcl(inode)){
      result = getOwnAclHelper(inode);
    } else {
      result = getInheritedDefaultAcl(inode);
    }
    if (result == null || result.size() == 0 ){
      return null;
    }
    return new AclFeature(result);
  }
  
  private static List<AclEntry> getOwnAclHelper(INode inode) throws IOException {
    if(inode.hasMoreAces()){
      return INodeAcls.getInodeAcl(inode.getId());
    } else {
      // get aces by pkey based on the cached references in the inode.
      List<Integer> aceIds = getCacheAceReferences(inode);
      return INodeAcls.getInodeAcesByIds(inode.getId(), aceIds);
    }
  }
  
  static boolean hasOwnAcl(INode inode){
    return inode.getAce1Id() != NON_EXISTING_ACE_ID || inode.getAce2Id() != NON_EXISTING_ACE_ID || inode.hasMoreAces();
  }
  
  static void addAclFeature(INode inode, AclFeature aclFeature) throws IOException {
    if (hasOwnAcl(inode)){
      throw new RuntimeException("Cannot add acl feature without removing the last one");
    }
    List<AclEntry> entries = aclFeature.getEntries();
    List<Integer> aceIds = INodeAcls.addInodeAcl(inode.getId(), entries);
    cacheAceReferences(inode, aceIds);
    inode.setHasMoreAces(entries.size() > 2);
  }
  
  private static void cacheAceReferences(INode inode, List<Integer> aceIds)
      throws TransactionContextException, StorageException {
    if (aceIds.size() > 0){
      inode.setAce1Id(aceIds.get(0));
    }
    if (aceIds.size() > 1) {
      inode.setAce2Id(aceIds.get(1));
    }
  }
  
  private static List<Integer> getCacheAceReferences(INode inode){
    List<Integer> cacheAceReferences = new ArrayList<>();
    int ace1Id = inode.getAce1Id();
    int ace2Id = inode.getAce2Id();
    
    if (ace2Id == NON_EXISTING_ACE_ID){
      if (ace1Id == NON_EXISTING_ACE_ID){
        return cacheAceReferences;
      }
      cacheAceReferences.add(ace1Id);
      return cacheAceReferences;
    }
    
    cacheAceReferences.add(ace1Id);
    cacheAceReferences.add(ace2Id);
    return cacheAceReferences;
  }
  
  static void removeAclFeature(INode inode) throws IOException {
    if (hasOwnAcl(inode)){
      INodeAcls.removeInodeAcl(inode.getId());
      inode.setAces(NON_EXISTING_ACE_ID, NON_EXISTING_ACE_ID, false);
    }
  }
  
  private static List<AclEntry> getInheritedDefaultAcl(INode inode) throws IOException {
    if (inode.getParentId() == ROOT_ID){
      //Root INode does not have acls.
      return null;
    }
    
    INode parent = inode.getParent();
    if (hasOwnAcl(parent)){
      List<AclEntry> aclEntries = readINodeAcl(parent);
      List<AclEntry> defaultEntries = new ArrayList<>();
      for (AclEntry aclEntry : aclEntries) {
        if (aclEntry.getScope().equals(AclEntryScope.DEFAULT)){
          defaultEntries.add(new AclEntry.Builder()
              .setType(aclEntry.getType())
              .setPermission(aclEntry.getPermission())
              .setScope(AclEntryScope.ACCESS)
              .setName(aclEntry.getName())
              .build());
        }
      }
      if (!defaultEntries.isEmpty()){
        return defaultEntries;
      }
    }
    
    return getInheritedDefaultAcl(parent);
  }
  
}
