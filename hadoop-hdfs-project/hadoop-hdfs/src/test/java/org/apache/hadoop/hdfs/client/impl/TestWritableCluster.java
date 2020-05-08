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
package org.apache.hadoop.hdfs.client.impl;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.junit.Test;

/**
 * Test writable cluster.
 */
public class TestWritableCluster {
  private final File siteFile;

  private MiniDFSCluster cluster;
  private URI fsURI;

  public TestWritableCluster() {
    String classPath = Thread.currentThread().getContextClassLoader()
                               .getResource("").getPath();
    siteFile = new File(classPath, "hdfs-site.xml");
    siteFile.delete();
  }

  private void startUpCluster() throws Exception {
    cluster = new MiniDFSCluster.Builder(new HdfsConfiguration())
        .numDataNodes(3).build();
    fsURI = cluster.getURI();
  }

  private void startUpHACluster() throws Exception {
    cluster = new MiniDFSCluster.Builder(new HdfsConfiguration())
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(3).build();
    cluster.waitActive();
    fsURI = HATestUtil.getLogicalUri(cluster);
  }

  private void shutDownCluster() throws Exception {
    siteFile.delete();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void prepareHdfsSiteXml(String writableNNs) throws Exception {
    if (writableNNs != null) {
      // prepare configuration hdfs-site.xml
      PrintWriter pw;
      pw = new PrintWriter(siteFile);
      pw.print("<?xml version=\"1.0\"?>\n"+
          "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"+
          "<configuration>\n"+
          " <property>\n"+
          "   <name>"
            + HdfsClientConfigKeys.DFS_CLIENT_WRITABLE_NAMENODE_LIST_KEY
            + "</name>\n"+
          "   <value>"+ writableNNs +"</value>\n"+
          " </property>\n"+
          "</configuration>\n");
      pw.close();
    }
  }

  private interface Work  {
    void run() throws Throwable;
  }

  private void testWritableClusterOP(Work work, boolean expectError) {
    Throwable ex = null;
    try {
      work.run();
    } catch (Throwable e) {
      ex = e;
    }

    if (expectError) {
      assertTrue(ex instanceof UnsupportedOperationException);
    } else {
      assertNull(ex);
    }
  }

  /*
   * has no writable namenodes conf, everything should be ok.
   */
  @SuppressWarnings("deprecation")
  private void testNonWritableConf(boolean ha) throws Exception {
    final DistributedFileSystem dfs;
    if (ha) {
      dfs = HATestUtil.configureFailoverFs(cluster, new Configuration());
      cluster.transitionToActive(0);
    } else {
      dfs = (DistributedFileSystem)FileSystem.get(fsURI, new Configuration());
    }

    final Path file = new Path("/file");
    final Path dir = new Path("/dir");
    Work work = null;

    // create
    work = new Work() {
      public void run() throws Throwable {
        dfs.create(file).close();
      };
    };
    testWritableClusterOP(work, false);

    // create symlink
    work = new Work() {
      public void run() throws Throwable {
        Path link = new Path("/test_symlink");
        dfs.createSymlink(file, link, true);
        dfs.delete(link, true);
      };
    };
    testWritableClusterOP(work, false);

    // append
    work = new Work() {
      public void run() throws Throwable {
        dfs.append(file).close();
      };
    };
    testWritableClusterOP(work, false);

    // rename
    work = new Work() {
      public void run() throws Throwable {
        Path newPath = new Path("/testRename");
        dfs.rename(file, newPath);
      };
    };
    testWritableClusterOP(work, false);

    // rename2
    work = new Work() {
      public void run() throws Throwable {
        Path newPath = new Path("/testRename");
        dfs.rename(newPath, file, Rename.OVERWRITE);
      };
    };
    testWritableClusterOP(work, false);

    // concat
    work = new Work() {
      public void run() throws Throwable {
        Path file2 = new Path("/file2");
        FSDataOutputStream out = dfs.create(file2);
        out.write("foo".getBytes());
        out.close();
        dfs.concat(file, new Path[] {file2});
      };
    };
    testWritableClusterOP(work, false);

    // truncate
    work = new Work() {
      public void run() throws Throwable {
        dfs.truncate(file, 0);
      };
    };
    testWritableClusterOP(work, false);

    // mkdir
    work = new Work() {
      public void run() throws Throwable {
        dfs.mkdirs(dir);
      }
    };
    testWritableClusterOP(work, false);

   // ls
    work = new Work() {
      public void run() throws Throwable {
        dfs.listFiles(new Path("/"), true);
      }
    };
    testWritableClusterOP(work, false);

    // delete
    work = new Work() {
      public void run() throws Throwable {
        dfs.delete(file);
      }
    };
    testWritableClusterOP(work, false);

    // delete2
    work = new Work() {
      public void run() throws Throwable {
        dfs.delete(dir, true);
      }
    };
    testWritableClusterOP(work, false);

    dfs.close();
  }

  /*
   * has writable namenodes conf but its empty, everything should be ok.
   */
  private void testEmptyWritableConf(boolean ha) throws Exception {
    testNonWritableConf(ha);
  }

  /*
   * has writable namenodes conf and it's value is localhost/localNS
   * everything should be ok.
   */
  private void testLocalNsWritableConf(boolean ha) throws Exception {
    testNonWritableConf(ha);
  }

  /*
   * has writable namenodes conf and it's value is not localhost/ns1,
   * so now we expect UnsupportedOperationException.
   */
  @SuppressWarnings("deprecation")
  private void testNonLocalNsWritableConf(boolean ha) throws Exception {
    final DistributedFileSystem dfs;
    if (ha) {
      dfs = HATestUtil.configureFailoverFs(cluster, new Configuration());
      cluster.transitionToActive(0);
    } else {
      dfs = (DistributedFileSystem)FileSystem.get(fsURI, new Configuration());
    }

    final Path file = new Path("/file");
    final Path dir = new Path("/dir");
    Work work = null;

    // create
    work = new Work() {
      public void run() throws Throwable {
        dfs.create(file).close();
      };
    };
    testWritableClusterOP(work, true);

    // create symlink
    work = new Work() {
      public void run() throws Throwable {
        dfs.createSymlink(file, new Path("/testLink/link"), true);
      };
    };
    testWritableClusterOP(work, true);

    // append
    work = new Work() {
      public void run() throws Throwable {
        dfs.append(file).close();
      };
    };
    testWritableClusterOP(work, true);

    // rename
    work = new Work() {
      public void run() throws Throwable {
        Path newPath = new Path("/testRename");
        dfs.rename(file, newPath);
      };
    };
    testWritableClusterOP(work, true);

    // rename2
    work = new Work() {
      public void run() throws Throwable {
        Path newPath = new Path("/testRename");
        dfs.rename(newPath, file, Rename.OVERWRITE);
      };
    };
    testWritableClusterOP(work, true);

    // concat
    work = new Work() {
      public void run() throws Throwable {
        dfs.concat(file, new Path[] {file});
      };
    };
    testWritableClusterOP(work, true);

    // truncate
    work = new Work() {
      public void run() throws Throwable {
        dfs.truncate(file, 0);
      };
    };
    testWritableClusterOP(work, true);

    // mkdir
    work = new Work() {
      public void run() throws Throwable {
        dfs.mkdirs(dir);
      }
    };
    testWritableClusterOP(work, true);

   // ls
    work = new Work() {
      public void run() throws Throwable {
        dfs.listFiles(new Path("/"), true);
      }
    };
    testWritableClusterOP(work, false);

    // delete
    work = new Work() {
      public void run() throws Throwable {
        dfs.delete(file);
      }
    };
    testWritableClusterOP(work, true);

    // delete2
    work = new Work() {
      public void run() throws Throwable {
        dfs.delete(dir, true);
      }
    };
    testWritableClusterOP(work, true);

    dfs.close();
  }

  // Test Non-HA cluster
  @Test
  public void testWritableCluster() throws Exception {
    startUpCluster();

    prepareHdfsSiteXml(null);
    testNonWritableConf(false);

    prepareHdfsSiteXml("");
    testEmptyWritableConf(false);

    prepareHdfsSiteXml(fsURI.getHost());
    testLocalNsWritableConf(false);

    prepareHdfsSiteXml("192.168.22.22");
    testNonLocalNsWritableConf(false);

    shutDownCluster();
  }

  // Test HA cluster
  @Test
  public void testWritableClusterHA() throws Exception {
    startUpHACluster();

    prepareHdfsSiteXml(null);
    testNonWritableConf(true);

    prepareHdfsSiteXml("");
    testEmptyWritableConf(true);

    prepareHdfsSiteXml(fsURI.getHost());
    testLocalNsWritableConf(true);

    prepareHdfsSiteXml("nsX");
    testNonLocalNsWritableConf(true);

    shutDownCluster();
  }
}