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

import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests NameNode interaction for all ACL modification APIs.  This test suite
 * also covers interaction of setPermission with inodes that have ACLs.
 */
public abstract class FSAclBaseTest {

  protected static MiniDFSCluster cluster;
  protected static FileSystem fs;
  private static int pathCount = 0;
  private static Path path;

  @AfterClass
  public static void shutdown() throws Exception {
    IOUtils.cleanup(null, fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() {
    pathCount += 1;
    path = new Path("/p" + pathCount);
  }

  @Test(expected=AclException.class)
  public void testModifyAclEntriesDefaultOnFile() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.modifyAclEntries(path, aclSpec);
  }

  @Test(expected=FileNotFoundException.class)
  public void testRemoveAclEntriesPathNotFound() throws IOException {
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"));
    fs.removeAclEntries(path, aclSpec);
  }

  @Test
  public void testRemoveDefaultAclOnlyDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0750);
    assertAclFeature(false);
  }

  @Test
  public void testRemoveDefaultAclMinimal() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0750);
    assertAclFeature(false);
  }

  //@Test
  public void testRemoveDefaultAclStickyBit() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission((short)01770);
    assertAclFeature(true);
  }

  @Test(expected=FileNotFoundException.class)
  public void testRemoveDefaultAclPathNotFound() throws IOException {
    // Path has not been created.
    fs.removeDefaultAcl(path);
  }

  //@Test
  public void testRemoveAcl() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.removeAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0750);
    assertAclFeature(false);
  }

  @Test
  public void testRemoveAclMinimalAcl() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    fs.removeAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0640);
    assertAclFeature(false);
  }

  //@Test
  public void testRemoveAclStickyBit() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.removeAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)01750);
    assertAclFeature(false);
  }

  //@Test
  public void testRemoveAclOnlyDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.removeAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0750);
    assertAclFeature(false);
  }

  @Test(expected=FileNotFoundException.class)
  public void testRemoveAclPathNotFound() throws IOException {
    // Path has not been created.
    fs.removeAcl(path);
  }

  //@Test
  public void testSetAcl() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)0770);
    assertAclFeature(true);
  }

  //@Test
  public void testSetAclOnlyAccess() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission((short)0640);
    assertAclFeature(true);
  }

  //@Test
  public void testSetAclOnlyDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)0750);
    assertAclFeature(true);
  }

  //@Test
  public void testSetAclMinimal() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0644));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0640);
    assertAclFeature(false);
  }

  //@Test
  public void testSetAclMinimalDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)0750);
    assertAclFeature(true);
  }

  //@Test
  public void testSetAclCustomMask() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, MASK, ALL),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission((short)0670);
    assertAclFeature(true);
  }

  //@Test
  public void testSetAclStickyBit() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)01770);
    assertAclFeature(true);
  }

  //@Test(expected=FileNotFoundException.class)
  public void testSetAclPathNotFound() throws IOException {
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testSetAclDefaultOnFile() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
  }

  //@Test
  public void testSetPermission() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.setPermission(path, FsPermission.createImmutable((short)0700));
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)0700);
    assertAclFeature(true);
  }

  //@Test
  public void testSetPermissionOnlyAccess() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    fs.setPermission(path, FsPermission.createImmutable((short)0600));
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission((short)0600);
    assertAclFeature(true);
  }

  //@Test
  public void testSetPermissionOnlyDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.setPermission(path, FsPermission.createImmutable((short)0700));
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)0700);
    assertAclFeature(true);
  }

  //@Test
  public void testDefaultAclNewFile() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path filePath = new Path(path, "file1");
    fs.create(filePath).close();
    AclStatus s = fs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission(filePath, (short)0640);
    assertAclFeature(filePath, true);
  }

  @Test
  public void testOnlyAccessAclNewFile() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", ALL));
    fs.modifyAclEntries(path, aclSpec);
    Path filePath = new Path(path, "file1");
    fs.create(filePath).close();
    AclStatus s = fs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(filePath, (short)0644);
    assertAclFeature(filePath, false);
  }

  //@Test
  public void testDefaultMinimalAclNewFile() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    Path filePath = new Path(path, "file1");
    fs.create(filePath).close();
    AclStatus s = fs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(filePath, (short)0640);
    assertAclFeature(filePath, false);
  }

  //@Test
  public void testDefaultAclNewDir() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    fs.mkdirs(dirPath);
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(dirPath, (short)0750);
    assertAclFeature(dirPath, true);
  }

  @Test
  public void testOnlyAccessAclNewDir() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", ALL));
    fs.modifyAclEntries(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    fs.mkdirs(dirPath);
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(dirPath, (short)0755);
    assertAclFeature(dirPath, false);
  }

  //@Test
  public void testDefaultMinimalAclNewDir() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    fs.mkdirs(dirPath);
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(dirPath, (short)0750);
    assertAclFeature(dirPath, true);
  }

  //@Test
  public void testDefaultAclNewFileIntermediate() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    Path filePath = new Path(dirPath, "file1");
    fs.create(filePath).close();
    AclEntry[] expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) };
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(dirPath, (short)0750);
    assertAclFeature(dirPath, true);
    expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) };
    s = fs.getAclStatus(filePath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(filePath, (short)0640);
    assertAclFeature(filePath, true);
  }

  //@Test
  public void testDefaultAclNewDirIntermediate() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    Path subdirPath = new Path(dirPath, "subdir1");
    fs.mkdirs(subdirPath);
    AclEntry[] expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) };
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(dirPath, (short)0750);
    assertAclFeature(dirPath, true);
    s = fs.getAclStatus(subdirPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(subdirPath, (short)0750);
    assertAclFeature(subdirPath, true);
  }

  //@Test
  public void testDefaultAclNewSymlinkIntermediate() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    Path filePath = new Path(path, "file1");
    fs.create(filePath).close();
    fs.setPermission(filePath, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    Path linkPath = new Path(dirPath, "link1");
    fs.createSymlink(filePath, linkPath, true);
    AclEntry[] expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) };
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(dirPath, (short)0750);
    assertAclFeature(dirPath, true);
    expected = new AclEntry[] { };
    s = fs.getAclStatus(linkPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(linkPath, (short)0640);
    assertAclFeature(linkPath, false);
    s = fs.getAclStatus(filePath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(filePath, (short)0640);
    assertAclFeature(filePath, false);
  }

  //@Test
  public void testDefaultAclNewFileWithMode() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path filePath = new Path(path, "file1");
    int bufferSize = cluster.getConfiguration(0).getInt(
      CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY,
      CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_DEFAULT);
    fs.create(filePath, new FsPermission((short)0740), false, bufferSize,
      fs.getDefaultReplication(filePath), fs.getDefaultBlockSize(path), null)
      .close();
    AclStatus s = fs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission(filePath, (short)0740);
    assertAclFeature(filePath, true);
  }

  //@Test
  public void testDefaultAclNewDirWithMode() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    fs.mkdirs(dirPath, new FsPermission((short)0740));
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, READ_EXECUTE) }, returned);
    assertPermission(dirPath, (short)0740);
    assertAclFeature(dirPath, true);
  }

  @Test
  public void testDefaultInheritedAfterChildCreation() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    Path newDir = new Path(path, "dir1");
    fs.mkdirs(newDir, new FsPermission((short)0740));
    ArrayList<AclEntry> aclEntries = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclEntries);
    AclStatus aclStatus = fs.getAclStatus(newDir);
    AclEntry[] returned = aclStatus.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[]{
        aclEntry(ACCESS, USER, "foo", ALL)
    }, returned);
  }
  
  @Test
  public void testDefaultInheritedGrandChild() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    Path newDir = new Path(path, "dir1");
    fs.mkdirs(newDir, new FsPermission((short)0740));
    Path newFile = new Path(newDir, "file1");
    int bufferSize = cluster.getConfiguration(0).getInt(
        CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY,
        CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_DEFAULT);
    fs.create(newFile, new FsPermission((short)0740), false, bufferSize,
        fs.getDefaultReplication(newFile), fs.getDefaultBlockSize(path), null)
        .close();
    
    ArrayList<AclEntry> aclEntries = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclEntries);
    AclStatus aclStatus = fs.getAclStatus(newFile);
    AclEntry[] returned = aclStatus.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[]{
        aclEntry(ACCESS, USER, "foo", ALL)
    }, returned);
  }
  
  @Test
  public void testUnnamedDefaultForbidden() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    ArrayList<AclEntry> aclEntries = Lists.newArrayList(
        aclEntry(DEFAULT, USER, ALL));
    try {
      fs.setAcl(path, aclEntries);
      fail("Set acl did not throw exception for unnamed default");
    } catch (Exception e){
      //expected
    }
  }
  
  @Test
  public void testMaskForbidden() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    ArrayList<AclEntry> aclEntries = Lists.newArrayList(
        aclEntry(ACCESS, MASK, ALL));
    try {
      fs.setAcl(path, aclEntries);
      fail("Set acl did not throw exception for unnamed default");
    } catch (Exception e){
      //expected
    }
    
    aclEntries = Lists.newArrayList(
        aclEntry(DEFAULT, MASK, ALL));
    try {
      fs.setAcl(path, aclEntries);
      fail("Set acl did not throw exception for unnamed default");
    } catch (Exception e){
      //expected
    }
  }
  
  @Test
  public void testInheritedDefaultRemovedWithAncestor() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    Path newDir = new Path(path, "dir1");
    fs.mkdirs(newDir, new FsPermission((short)0740));
    ArrayList<AclEntry> aclEntries = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclEntries);
    AclStatus aclStatus = fs.getAclStatus(newDir);
    AclEntry[] returned = aclStatus.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[]{
        aclEntry(ACCESS, USER, "foo", ALL)
    }, returned);
    
    //Now remove ancestor default acl and check again
    fs.removeAcl(path);
    
    aclStatus = fs.getAclStatus(newDir);
    returned = aclStatus.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[0], returned);
    
  }
  
  @Test
  public void testInheritedDefaultBlockedByIntermediateAncestor() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    Path newDir = new Path(path, "dir1");
    fs.mkdirs(newDir, new FsPermission((short)0740));
    ArrayList<AclEntry> aclEntries = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclEntries);
    
    Path newDir2 = new Path(newDir, "dir2");
    fs.mkdirs(newDir2);
    
    aclEntries = Lists.newArrayList(
        aclEntry(DEFAULT, GROUP, "goo", NONE),
        aclEntry(DEFAULT, USER, "foot", READ_EXECUTE)
    );
    fs.setAcl(newDir, aclEntries);
    
    AclStatus aclStatus = fs.getAclStatus(newDir2);
    AclEntry[] returned = aclStatus.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[]{
        aclEntry(ACCESS, USER, "foot", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "goo", NONE)
    }, returned);
  }
  
  @Test
  public void testAclSorting() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    
    ArrayList<AclEntry> aclEntries = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(ACCESS, GROUP, "goo", READ_WRITE),
        aclEntry(DEFAULT, GROUP, "foo", ALL),
        aclEntry(ACCESS, USER, "goo", READ_WRITE));
    fs.setAcl(path, aclEntries);
  
    List<AclEntry> returned = fs.getAclStatus(path).getEntries();
    assertArrayEquals(new AclEntry[]{
        aclEntry(ACCESS, USER, "goo", READ_WRITE),
        aclEntry(ACCESS, GROUP, "goo", READ_WRITE),
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(DEFAULT, GROUP, "foo", ALL)
    }, returned.toArray(new AclEntry[0]));
  }
  
  @Test
  public void testRemoveOnlyOneOfDefaultAclEntries() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    Path newDir = new Path(path, "dir1");
    fs.mkdirs(newDir, new FsPermission((short)0740));
    ArrayList<AclEntry> aclEntries = Lists.newArrayList(
        aclEntry(ACCESS, GROUP, "hey", READ_EXECUTE),
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(DEFAULT, USER, "robin", NONE));
    fs.setAcl(path, aclEntries);
    
    ArrayList<AclEntry> toRemove = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo"));
    fs.removeAclEntries(path, toRemove);
  
    List<AclEntry> afterRemove = fs.getAclStatus(path).getEntries();
    assertArrayEquals(new AclEntry[]{
        aclEntry(ACCESS, GROUP, "hey", READ_EXECUTE),
        aclEntry(DEFAULT, USER, "robin", NONE)
    }, afterRemove.toArray(new AclEntry[0]));
  }
  
  @Test
  public void testAccessAclBlocksInheritance() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    Path newDir = new Path(path, "dir1");
    fs.mkdirs(newDir, new FsPermission((short)0740));
    
    ArrayList<AclEntry> aclEntries = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclEntries);
  
    AclStatus aclStatus = fs.getAclStatus(newDir);
    AclEntry[] returned = aclStatus.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[]{
        aclEntry(ACCESS, USER, "foo", ALL)
    }, returned);
    
    
    aclEntries = Lists.newArrayList(
        aclEntry(ACCESS, USER, "someone", NONE)
    );
    fs.setAcl(newDir, aclEntries);
  
    aclStatus = fs.getAclStatus(newDir);
    returned = aclStatus.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[]{
        aclEntry(ACCESS, USER, "someone", NONE)
    }, returned);
    
  }
  
  @Test
  public void testRemoveOnlyDefaultEntries() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    Path newDir = new Path(path, "dir1");
    fs.mkdirs(newDir, new FsPermission((short)0740));
    ArrayList<AclEntry> aclEntries = Lists.newArrayList(
        aclEntry(ACCESS, GROUP, "hey", READ_EXECUTE),
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(DEFAULT, USER, "robin", NONE));
    fs.setAcl(path, aclEntries);
    
    fs.removeDefaultAcl(path);
  
    List<AclEntry> afterRemove = fs.getAclStatus(path).getEntries();
    assertArrayEquals(new AclEntry[]{
        aclEntry(ACCESS, GROUP, "hey", READ_EXECUTE)
    }, afterRemove.toArray(new AclEntry[0]));
  }
  
  @Test
  public void testRemoveDefaultOnlyAccessEntries() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    Path newDir = new Path(path, "dir1");
    fs.mkdirs(newDir, new FsPermission((short)0740));
    ArrayList<AclEntry> aclEntries = Lists.newArrayList(
        aclEntry(ACCESS, USER, "foo", ALL),
        aclEntry(ACCESS, GROUP, "hey", READ_EXECUTE),
        aclEntry(ACCESS, USER, "robin", NONE));
    fs.setAcl(path, aclEntries);
  
    fs.removeDefaultAcl(path);
  
    List<AclEntry> afterRemove = fs.getAclStatus(path).getEntries();
    assertArrayEquals(new AclEntry[]{
        aclEntry(ACCESS, USER, "foo", ALL),
        aclEntry(ACCESS, USER, "robin", NONE),
        aclEntry(ACCESS, GROUP, "hey", READ_EXECUTE)
    }, afterRemove.toArray(new AclEntry[0]));
  }
  
  @Test
  public void testStickyBitAfterAclRemoved() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, "foo", ALL),
        aclEntry(ACCESS, GROUP, "goo", ALL));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", ALL),
        aclEntry(ACCESS, GROUP, "goo", ALL)}, returned);
    fs.removeAcl(path);
    assertPermission((short)01750);
    assertAclFeature(false);
    
  }
  
  @Test
  public void testStickyBitAfterDefaultAclRemoved() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(DEFAULT, GROUP, "goo", ALL));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(DEFAULT, GROUP, "goo", ALL)}, returned);
    fs.removeDefaultAcl(path);
    assertPermission((short)01750);
    assertAclFeature(false);
  }
  
  @Test
  public void testDisallowAccessEntryNamedWithFileUserOrGroup() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)01750));
    FileStatus fileStatus = fs.getFileStatus(path);
    try{
      fs.setAcl(path, Lists.newArrayList(aclEntry(ACCESS, USER, fileStatus.getOwner(), NONE )));
      fail("Setting user access entry with name of file owner should throw exception");
    } catch (Exception e){
      //expected
    }
    try{
      fs.setAcl(path, Lists.newArrayList(aclEntry(ACCESS, GROUP, fileStatus.getGroup(), NONE )));
      fail("Setting group access entry with name of file group should throw exception");
    } catch (Exception e){
      //expected
    }
  }
  
  @Test(expected=FileNotFoundException.class)
  public void testSetAclFileNotFound() throws IOException {
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, "foo", READ),
        aclEntry(DEFAULT, GROUP, "goo", NONE));
    fs.setAcl(path, aclSpec);
  }
  
  @Test(expected=FileNotFoundException.class)
  public void testModifyAclPathNotFound() throws IOException {
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, "foo", ALL));
    fs.modifyAclEntries(path, aclSpec);
  }
  
  
  /**
   * Asserts whether or not the inode for the test path has an AclFeature.
   *
   * @param expectAclFeature boolean true if an AclFeature must be present,
   *   false if an AclFeature must not be present
   * @throws IOException thrown if there is an I/O error
   */
  private static void assertAclFeature(boolean expectAclFeature)
      throws IOException {
    assertAclFeature(path, expectAclFeature);
  }

  /**
   * Asserts whether or not the inode for a specific path has an AclFeature.
   *
   * @param pathToCheck Path inode to check
   * @param expectAclFeature boolean true if an AclFeature must be present,
   *   false if an AclFeature must not be present
   * @throws IOException thrown if there is an I/O error
   */
  private static void assertAclFeature(final Path pathToCheck,
      final boolean expectAclFeature) throws IOException {
    
    new HopsTransactionalRequestHandler(HDFSOperationType.GET_INODE){
  
      @Override
      public Object performTask() throws IOException {
        INode inode = cluster.getNamesystem().getINode(pathToCheck.toUri().getPath());
        assertNotNull(inode);
        List<AclEntry> aclEntries = AclStorage.readINodeAcl(inode);
        if (expectAclFeature) {
          assertFalse(aclEntries.isEmpty());
        } else {
          assertTrue(aclEntries.isEmpty());
        }
        return null;
      }
  
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getINodeLock(false, cluster.getNameNode(), TransactionLockTypes.INodeLockType.READ_COMMITTED,
            TransactionLockTypes.INodeResolveType.PATH, pathToCheck.toString()));
      }
    }.handle();
   
  }

  /**
   * Asserts the value of the FsPermission bits on the inode of the test path.
   *
   * @param perm short expected permission bits
   * @throws IOException thrown if there is an I/O error
   */
  private static void assertPermission(short perm) throws IOException {
    assertPermission(path, perm);
  }

  /**
   * Asserts the value of the FsPermission bits on the inode of a specific path.
   *
   * @param pathToCheck Path inode to check
   * @param perm short expected permission bits
   * @throws IOException thrown if there is an I/O error
   */
  private static void assertPermission(Path pathToCheck, short perm)
      throws IOException {
    AclTestHelpers.assertPermission(fs, pathToCheck, perm);
  }
}
