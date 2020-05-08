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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.junit.After;
import org.junit.Test;

/**
 * Test independent invalidation monitor.
 */
public class TestIndependentInvalidation {
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;

  private void setupCluster(Configuration conf) throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitClusterUp();
    dfs = cluster.getFileSystem();
  }

  @After
  public void shutdownCluster() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      dfs.close();
    }
  }

  @Test
  public void testNormal() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_BLOCK_INDEPENDENT_KEY, false);
    setupCluster(conf);

    test();
  }

  @Test
  public void testIndependent() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_BLOCK_INDEPENDENT_KEY, true);
    setupCluster(conf);

    test();
  }

  private void test() throws Exception {
    Path file = new Path("/test");
    FSDataOutputStream out = dfs.create(file);
    out.write("foooo".getBytes());
    out.close();

    String bpid1 = cluster.getNamesystem().getBlockPoolId();

    File storageDir1 = cluster.getInstanceStorageDir(0, 0);
    File storageDir2 = cluster.getInstanceStorageDir(1, 0);
    File storageDir3 = cluster.getInstanceStorageDir(2, 0);

    LocatedBlocks blocks = dfs.getClient().getLocatedBlocks(file.toString(), 0);
    long blockId = blocks.get(0).getBlock().getBlockId();

    verifyBlockFile(true, storageDir1, bpid1, blockId);
    verifyBlockFile(true, storageDir2, bpid1, blockId);
    verifyBlockFile(true, storageDir3, bpid1, blockId);

    dfs.delete(file, true);
    Thread.sleep(20000);

    verifyBlockFile(false, storageDir1, bpid1, blockId);
    verifyBlockFile(false, storageDir2, bpid1, blockId);
    verifyBlockFile(false, storageDir3, bpid1, blockId);
  }

  private void verifyBlockFile(boolean shouldExist,
      File storageDir, String bpid, long blockId) throws IOException {
    File bpDir = new File(storageDir, DataStorage.STORAGE_DIR_CURRENT + "/"
        + bpid);
    File bpCurrentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT);
    File finalizedDir = new File(bpCurrentDir,
        DataStorage.STORAGE_DIR_FINALIZED);
    File blockDir= DatanodeUtil.idToBlockDir(finalizedDir, blockId);
    File blockFile = new File(blockDir,
        Block.BLOCK_FILE_PREFIX + String.valueOf(blockId));

    if (shouldExist) {
      assertTrue(blockFile.exists());
    } else {
      assertFalse(blockFile.exists());
    }
  }
}