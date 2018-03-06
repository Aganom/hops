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
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

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
    AclFeature f = getAclFeature(inode);
    return f == null ? ImmutableList.<AclEntry> of() : f.getEntries();
  }

  /**
   * Reads the existing ACL of an inode.  This method always returns the full
   * logical ACL of the inode after reading relevant data from the inode's
   * {@link FsPermission} and {@link AclFeature}.  Note that every inode
   * logically has an ACL, even if no ACL has been set explicitly.  If the inode
   * does not have an extended ACL, then the result is a minimal ACL consising of
   * exactly 3 entries that correspond to the owner, group and other permissions.
   * This method always reads the inode's current state and does not support
   * querying by snapshot ID.  This is because the method is intended to support
   * ACL modification APIs, which always apply a delta on top of current state.
   *
   * @param inode INode to read
   * @return List<AclEntry> containing all logical inode ACL entries
   */
  public static List<AclEntry> readINodeLogicalAcl(INode inode) throws IOException {
    FsPermission perm = inode.getFsPermission();
    AclFeature f = getAclFeature(inode);
    if (f == null) {
      return getMinimalAcl(perm);
    }

    final List<AclEntry> existingAcl;
    // Split ACL entries stored in the feature into access vs. default.
    List<AclEntry> featureEntries = f.getEntries();
    ScopedAclEntries scoped = new ScopedAclEntries(featureEntries);
    List<AclEntry> accessEntries = scoped.getAccessEntries();
    List<AclEntry> defaultEntries = scoped.getDefaultEntries();

    // Pre-allocate list size for the explicit entries stored in the feature
    // plus the 3 implicit entries (owner, group and other) from the permission
    // bits.
    existingAcl = Lists.newArrayListWithCapacity(featureEntries.size() + 3);

    if (!accessEntries.isEmpty()) {
      // Add owner entry implied from user permission bits.
      existingAcl.add(new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.USER).setPermission(perm.getUserAction())
          .build());

      // Next add all named user and group entries taken from the feature.
      existingAcl.addAll(accessEntries);

      // Add mask entry implied from group permission bits.
      existingAcl.add(new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.MASK).setPermission(perm.getGroupAction())
          .build());

      // Add other entry implied from other permission bits.
      existingAcl.add(new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.OTHER).setPermission(perm.getOtherAction())
          .build());
    } else {
      // It's possible that there is a default ACL but no access ACL. In this
      // case, add the minimal access ACL implied by the permission bits.
      existingAcl.addAll(getMinimalAcl(perm));
    }

    // Add all default entries after the access entries.
    existingAcl.addAll(defaultEntries);

    // The above adds entries in the correct order, so no need to sort here.
    return existingAcl;
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

    FsPermission perm = inode.getFsPermission();
    List<AclEntry> featureEntries = f.getEntries();
    if (featureEntries.get(0).getScope() == AclEntryScope.ACCESS) {
      // Restore group permissions from the feature's entry to permission
      // bits, overwriting the mask, which is not part of a minimal ACL.
      AclEntry groupEntryKey = new AclEntry.Builder()
          .setScope(AclEntryScope.ACCESS).setType(AclEntryType.GROUP).build();
      int groupEntryIndex = Collections.binarySearch(featureEntries,
          groupEntryKey, AclTransformation.ACL_ENTRY_COMPARATOR);
      assert groupEntryIndex >= 0;
      FsAction groupPerm = featureEntries.get(groupEntryIndex).getPermission();
      FsPermission newPerm = new FsPermission(perm.getUserAction(), groupPerm,
          perm.getOtherAction(), perm.getStickyBit());
      inode.setPermission(newPerm);//, snapshotId);
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
    assert newAcl.size() >= 3;
    FsPermission perm = inode.getFsPermission();
    final FsPermission newPerm;
    if (!isMinimalAcl(newAcl)) {
      // This is an extended ACL.  Split entries into access vs. default.
      ScopedAclEntries scoped = new ScopedAclEntries(newAcl);
      List<AclEntry> accessEntries = scoped.getAccessEntries();
      List<AclEntry> defaultEntries = scoped.getDefaultEntries();

      // Only directories may have a default ACL.
      if (!defaultEntries.isEmpty() && !inode.isDirectory()) {
        throw new AclException(
          "Invalid ACL: only directories may have a default ACL.");
      }
      
      checkNoDisallowedEntries(newAcl);

      // Attach entries to the feature.
      if (hasOwnExtendedAcl(inode)) {
        removeAclFeature(inode);
      }
      
      addAclFeature(inode, createAclFeature(accessEntries, defaultEntries));
      newPerm = createFsPermissionForExtendedAcl(accessEntries, perm);
    } else {
      // This is a minimal ACL.  Remove the ACL feature if it previously had one.
      if (hasOwnExtendedAcl(inode)) {
        removeAclFeature(inode);
      }
      newPerm = createFsPermissionForMinimalAcl(newAcl, perm);
    }

    inode.setPermission(newPerm);//, snapshotId);
  }

  private static void checkNoDisallowedEntries(List<AclEntry> newAcl) throws AclException {
    for (AclEntry aclEntry : newAcl) {
      //Only allow named default entries
      if (aclEntry.getScope().equals(AclEntryScope.DEFAULT)
          && (aclEntry.getName() == null || aclEntry.getName().isEmpty())){
        throw new AclException("HOPS does not allowed unnamed DEFAULT entries");
      }
    }
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
  private static AclFeature createAclFeature(List<AclEntry> accessEntries,
      List<AclEntry> defaultEntries) {
    // Pre-allocate list size for the explicit entries stored in the feature,
    // which is all entries minus the 3 entries implicitly stored in the
    // permission bits.
    List<AclEntry> featureEntries = Lists.newArrayListWithCapacity(
      (accessEntries.size() - 3) + defaultEntries.size());

    // For the access ACL, the feature only needs to hold the named user and
    // group entries.  For a correctly sorted ACL, these will be in a
    // predictable range.
    if (!isMinimalAcl(accessEntries)) {
      featureEntries.addAll(
        accessEntries.subList(1, accessEntries.size() - 2));
    }

    // Add all default entries to the feature.
    featureEntries.addAll(defaultEntries);
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
    return new FsPermission(accessEntries.get(0).getPermission(),
      accessEntries.get(accessEntries.size() - 2).getPermission(),
      accessEntries.get(accessEntries.size() - 1).getPermission(),
      existingPerm.getStickyBit());
  }

  /**
   * Creates the new FsPermission for an inode that is receiving a minimal ACL,
   * based on its access ACL entries.  For a correctly sorted ACL, the owner,
   * group and other permissions are in order.  Also preserve sticky bit and
   * toggle ACL bit off.
   *
   * @param accessEntries List<AclEntry> access ACL entries
   * @param existingPerm FsPermission existing permissions
   * @return FsPermission new permissions
   */
  private static FsPermission createFsPermissionForMinimalAcl(
      List<AclEntry> accessEntries, FsPermission existingPerm) {
    return new FsPermission(accessEntries.get(0).getPermission(),
      accessEntries.get(1).getPermission(),
      accessEntries.get(2).getPermission(),
      existingPerm.getStickyBit());
  }

  /**
   * Translates the given permission bits to the equivalent minimal ACL.
   *
   * @param perm FsPermission to translate
   * @return List<AclEntry> containing exactly 3 entries representing the owner,
   *   group and other permissions
   */
  private static List<AclEntry> getMinimalAcl(FsPermission perm) {
    return Lists.newArrayList(
      new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.USER)
        .setPermission(perm.getUserAction())
        .build(),
      new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.GROUP)
        .setPermission(perm.getGroupAction())
        .build(),
      new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.OTHER)
        .setPermission(perm.getOtherAction())
        .build());
  }

  /**
   * Checks if the given entries represent a minimal ACL (contains exactly 3
   * entries).
   *
   * @param entries List<AclEntry> entries to check
   * @return boolean true if the entries represent a minimal ACL
   */
  private static boolean isMinimalAcl(List<AclEntry> entries) {
    return entries.size() == 3;
  }
  
  private static void removeAclFeature(INode inode) throws IOException {
    INodeAces.removeInodeAces(inode.id);
  }
  
  private static boolean hasOwnExtendedAcl(INode inode) throws IOException {
    INodeAces.getInodeAcl(inode);
    return !INodeAces.getInodeAcl(inode).isEmpty();
  }
  
  private static void addAclFeature(INode inode, AclFeature aclFeature) throws IOException {
    INodeAces.addInodeAces(inode.id, aclFeature.getEntries());
  }
  
  /**
   * HOPS: Get the extended acl entries for an inode.
   * This method recursively checks the ancestor path for default aces, in case the requested
   * inode doesn't have an extended acl itself.
   *
   * @return the corresponding AclFeature if any, otherwise null.
   */
  public static AclFeature getAclFeature(INode inode) throws IOException {
    List<AclEntry> inodeAcl = INodeAces.getInodeAcl(inode);
    if (inodeAcl.isEmpty()){
      inodeAcl = getInheritedAces(inode.parent);
    }
    
    if (inodeAcl.isEmpty()){
      //No inherited aces found
      return null;
    }
    
    return new AclFeature(inodeAcl);
  }
  
  private static List<AclEntry> getInheritedAces(INode inode)
      throws IOException {
    if (inode == null){
      return Lists.newArrayList();
    }
    List<AclEntry> inodeAcl = INodeAces.getInodeAcl(inode);
    
    List<AclEntry> convertedEntries = convertDefaultToAccess(extractDefaultEntries(inodeAcl));
    if (convertedEntries.isEmpty()){
      return getInheritedAces(inode.parent);
    }
    return convertedEntries;
  }
  
  private static List<AclEntry> extractDefaultEntries(List<AclEntry> aclEntries){
    List<AclEntry> defaultEntries = Lists.newArrayList();
    for (AclEntry aclEntry : aclEntries) {
      if (aclEntry.getScope().equals(AclEntryScope.DEFAULT)){
        defaultEntries.add(aclEntry);
      }
    }
    return defaultEntries;
  }
  
  private static List<AclEntry> convertDefaultToAccess(List<AclEntry> entries){
    List<AclEntry> convertedEntries = Lists.newArrayList();
    for (AclEntry entry : entries) {
      convertedEntries.add(new AclEntry.Builder().setType(entry.getType()).setPermission(entry.getPermission())
          .setName(entry.getName()).setScope(AclEntryScope.ACCESS).build());

    }
    return convertedEntries;
  }
}
