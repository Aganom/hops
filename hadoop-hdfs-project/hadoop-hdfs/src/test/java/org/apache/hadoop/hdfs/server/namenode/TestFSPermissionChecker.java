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

import static io.hops.transaction.lock.LockFactory.getInstance;
import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import io.hops.common.INodeUtil;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import static org.mockito.Matchers.any;

/**
 * Unit tests covering FSPermissionChecker.  All tests in this suite have been
 * cross-validated against Linux setfacl/getfacl to check for consistency of the
 * HDFS implementation.
 */
public class TestFSPermissionChecker {
  private static final long PREFERRED_BLOCK_SIZE = 128 * 1024 * 1024;
  private static final short REPLICATION = 3;
  private static final String SUPERGROUP = "supergroup";
  private static final String SUPERUSER = "superuser";
  private static final UserGroupInformation BRUCE =
    UserGroupInformation.createUserForTesting("bruce", new String[] { });
  private static final UserGroupInformation DIANA =
    UserGroupInformation.createUserForTesting("diana", new String[] { "sales" });
  private static final UserGroupInformation CLARK =
    UserGroupInformation.createUserForTesting("clark", new String[] { "execs" });

  private Configuration conf;
  private MiniDFSCluster cluster;
  private INodeDirectoryWithQuota inodeRoot;
  
  @Before
  public void setUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    new HopsTransactionalRequestHandler(HDFSOperationType.GET_ROOT){
      @Override
      public Object performTask() throws IOException {
        inodeRoot = cluster.getNamesystem().dir.getRootDir();
        return null;
      }
  
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getINodeLock(!cluster.getNamesystem().dir.isQuotaEnabled()/*skip INode Attr Lock*/, cluster
                .getNameNode(),
            TransactionLockTypes.INodeLockType.READ_COMMITTED,
            TransactionLockTypes.INodeResolveType.PATH, "/"));
      }
    }.handle();
    
  }
  
  @After
  public void tearDown() throws IOException {
    cluster.getFileSystem().close();
    cluster.shutdown();
  }

  @Test
  public void testAclNamedUser() throws IOException {
    INodeFile inodeFile = createINodeFile("/file1", "bruce", "execs",
      (short)0640);
    addAcl(inodeFile,
      //aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "diana", READ));
      //aclEntry(ACCESS, GROUP, READ),
      //aclEntry(ACCESS, MASK, READ),
      //aclEntry(ACCESS, OTHER, NONE));
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }

  @Test
  public void testAclNamedUserDeny() throws IOException {
    INodeFile inodeFile = createINodeFile("/file1", "bruce", "execs",
      (short)0644);
    addAcl(inodeFile,
//      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "diana", NONE));
//      aclEntry(ACCESS, GROUP, READ),
//      aclEntry(ACCESS, MASK, READ),
//      aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", READ);
  }

  @Test
  public void testAclNamedUserTraverseDeny() throws IOException {
    INodeDirectory inodeDir = createINodeDirectory(inodeRoot, "dir1", "bruce",
      "execs", (short)0755);
    INodeFile inodeFile = createINodeFile("/dir1/file1", "bruce", "execs",
      (short)0644);
    addAcl(inodeDir,
//      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "diana", NONE));
//      aclEntry(ACCESS, GROUP, READ_EXECUTE),
//      aclEntry(ACCESS, MASK, READ_EXECUTE),
//      aclEntry(ACCESS, OTHER, READ_EXECUTE));
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", ALL);
  }

  @Test
  public void testAclNamedGroup() throws IOException {
    INodeFile inodeFile = createINodeFile("/file1", "bruce", "execs",
      (short)0640);
    addAcl(inodeFile,
//      aclEntry(ACCESS, USER, READ_WRITE),
//      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, GROUP, "sales", READ));
//      aclEntry(ACCESS, MASK, READ),
//      aclEntry(ACCESS, OTHER, NONE));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }

  @Test
  public void testAclNamedGroupDeny() throws IOException {
    INodeFile inodeFile = createINodeFile("/file1", "bruce", "sales",
      (short)0644);
    addAcl(inodeFile,
//      aclEntry(ACCESS, USER, READ_WRITE),
//      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, GROUP, "execs", NONE));
//      aclEntry(ACCESS, MASK, READ),
//      aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }

  @Test
  public void testAclNamedGroupTraverseDeny() throws IOException {
    INodeDirectory inodeDir = createINodeDirectory(inodeRoot, "dir1", "bruce",
      "execs", (short)0755);
    INodeFile inodeFile = createINodeFile("/dir1/file1", "bruce", "execs",
      (short)0644);
    addAcl(inodeDir,
//      aclEntry(ACCESS, USER, ALL),
//      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "sales", NONE));
//      aclEntry(ACCESS, MASK, READ_EXECUTE),
//      aclEntry(ACCESS, OTHER, READ_EXECUTE));
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", ALL);
  }
  

  @Test
  public void testAclOther() throws IOException {
    INodeFile inodeFile = createINodeFile("/file1", "bruce", "sales",
      (short)0774);
    addAcl(inodeFile,
//      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "diana", ALL));
//      aclEntry(ACCESS, GROUP, READ_WRITE),
//      aclEntry(ACCESS, MASK, ALL),
//      aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", ALL);
    assertPermissionGranted(DIANA, "/file1", ALL);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }
  
  @Test
  public void testAclInheritedDefaultDeny() throws IOException {
    INodeDirectory inodeDir = createINodeDirectory(inodeRoot, "dir1", "bruce",
        "execs", (short)0777);
    INodeFile inodeFile = createINodeFile("/dir1/file1", "bruce", "execs",
        (short)0777);
    addAcl(inodeDir,
        //      aclEntry(ACCESS, USER, ALL),
        //      aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, "sales", READ));
    //      aclEntry(ACCESS, MASK, READ_EXECUTE),
    //      aclEntry(ACCESS, OTHER, READ_EXECUTE));
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/dir1/file1", READ);
    assertPermissionGranted(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", ALL);
  }

  private void addAcl(final INode inode, final AclEntry... acl)
      throws IOException {
    if (inode == null){
      throw new RuntimeException("Inode cannot be null");
    }
    new HopsTransactionalRequestHandler(HDFSOperationType.ADD_INODE_ACE) {
        String path;
        @Override
        public void setUp() throws StorageException {
          LinkedList<INode> inodesInPath = new LinkedList<>();
          INodeUtil.findPathINodesById(inode.getId(), inodesInPath , new boolean[1]);
          path = INodeUtil.constructPath(inodesInPath);
        }
          @Override
          public Object performTask() throws IOException {
            AclStorage.updateINodeAcl(inode, Arrays.asList(acl));
            return null;
          }
  
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            locks.add(lf.getINodeLock(cluster.getNameNode(), TransactionLockTypes.INodeLockType.WRITE,
                TransactionLockTypes.INodeResolveType.PATH, path));
          }
        }.handle();
    
  }

  private void assertPermissionGranted(final UserGroupInformation user, final String path,
      final FsAction access) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.CHECK_ACCESS){
  
      @Override
      public Object performTask() throws IOException {
        new FSPermissionChecker(SUPERUSER, SUPERGROUP, user).checkPermission(path,
            inodeRoot, false, null, null, access, null);
        return null;
      }
  
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getINodeLock(false/*skip quota*/, cluster.getNameNode(),
            TransactionLockTypes.INodeLockType.READ,
            TransactionLockTypes.INodeResolveType.PATH, false, path));
      }
    }.handle();
  }

  private void assertPermissionDenied(final UserGroupInformation user, final String path,
      final FsAction access) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.CHECK_ACCESS){
    
      @Override
      public Object performTask() throws IOException {
        try {
          new FSPermissionChecker(SUPERUSER, SUPERGROUP, user).checkPermission(path,
              inodeRoot, false, null, null, access, null);
          fail("expected AccessControlException for user + " + user + ", path = " +
              path + ", access = " + access);
        } catch (AccessControlException e) {
          // expected
        }
        return null;
      }
    
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getINodeLock(true/*skip quota*/, cluster.getNameNode(),
            TransactionLockTypes.INodeLockType.READ,
            TransactionLockTypes.INodeResolveType.PATH, true, path));
      }
    }.handle();
    
  }

  private INodeDirectory createINodeDirectory(final INodeDirectory parent,
      final String name, String owner, String group, short perm) throws IOException {
    final PermissionStatus permStatus = PermissionStatus.createImmutable(owner, group,
      FsPermission.createImmutable(perm));
    final Path src = new Path(parent.getFullPathName() + name);
    
    new HopsTransactionalRequestHandler(HDFSOperationType.ADD_INODE){
  
      @Override
      public Object performTask() throws IOException {
        cluster.getNamesystem().dir.mkdirs(src.toString(), permStatus, true, System.nanoTime());
        return null;
      }
  
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getINodeLock(false/*skip quota*/, cluster.getNameNode(),
            TransactionLockTypes.INodeLockType.WRITE,
            TransactionLockTypes.INodeResolveType.PATH, true, src.toString()));
      }
    }.handle();
  
    return (INodeDirectory) new HopsTransactionalRequestHandler(HDFSOperationType.GET_INODE){
      @Override
      public Object performTask() throws IOException {
        return cluster.getNamesystem().getINode(src.toString());
      }
    
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getINodeLock(true, cluster.getNameNode(), TransactionLockTypes.INodeLockType.READ_COMMITTED,
            TransactionLockTypes.INodeResolveType.PATH, src.toString()));
      }
    }.handle();
  }

  private INodeFile createINodeFile(String path,
      String owner, String group, short perm) throws IOException {

    final Path src = new Path(path);
    System.out.println("Creating new file: " + src);
    DFSTestUtil.createFile(cluster.getFileSystem(), src, 0, REPLICATION,0);
    cluster.getNamesystem().setOwner(src.toString(),owner,group);
    cluster.getNamesystem().setPermission(src.toString(), FsPermission.createImmutable(perm));
    
    return (INodeFile) new HopsTransactionalRequestHandler(HDFSOperationType.GET_INODE){
      @Override
      public Object performTask() throws IOException {
        return cluster.getNamesystem().getINode(src.toString());
      }
  
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getINodeLock(true, cluster.getNameNode(), TransactionLockTypes.INodeLockType.READ_COMMITTED,
            TransactionLockTypes.INodeResolveType.PATH, src.toString()));
      }
    }.handle();
  }
}
