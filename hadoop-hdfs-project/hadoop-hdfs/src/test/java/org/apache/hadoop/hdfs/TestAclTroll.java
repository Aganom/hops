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
package org.apache.hadoop.hdfs;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.AceDataAccess;
import io.hops.metadata.hdfs.entity.Ace;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestAclTroll {
  @Test
  public void testAclCreation() throws IOException {
    HdfsStorageFactory.setConfiguration(new Configuration());
    HdfsStorageFactory.formatStorage();
    
    final Ace toAdd = new Ace(-1, 1,"hej", Ace.AceType.USER, false, 0,0);
    
    Ace added = (Ace) new LightWeightRequestHandler(
        HDFSOperationType.COUNTALL_FILES) {
      @Override
      public Object performTask() throws IOException {
        AceDataAccess ida = (AceDataAccess) HdfsStorageFactory
            .getDataAccess(AceDataAccess.class);
        return ida.addAce(toAdd);
      }
    }.handle();
    
    assertNotEquals("the new persisted ace should have a new id, not -1", added.getId(), -1);
  }
  
  @Test
  public void testAclListing() throws IOException {
    //HdfsStorageFactory.setConfiguration(new Configuration());
    HdfsStorageFactory.formatStorage();
    
    for (int i = 2 ; i < 12 ; i++){
      final int index = i;
      new LightWeightRequestHandler(HDFSOperationType.COUNT_NODES) {
        @Override
        public Object performTask() throws IOException {
          AceDataAccess ida = (AceDataAccess) HdfsStorageFactory
              .getDataAccess(AceDataAccess.class);
          return ida.addAce(new Ace(-1, index, "hej", Ace.AceType.USER, false, 1,0));
        }
      }.handle();
    }
  
    List<Ace> aces = (List<Ace>) new LightWeightRequestHandler(HDFSOperationType.COUNT_NODES) {
      @Override
      public Object performTask() throws IOException {
        AceDataAccess ida = (AceDataAccess) HdfsStorageFactory
            .getDataAccess(AceDataAccess.class);
        return ida.getAcesByInodeId(10);
      }
    }.handle();
    
    
    assertEquals("added 10 aces, should be exactly 10 returned", 10, aces.size());
  }
}
