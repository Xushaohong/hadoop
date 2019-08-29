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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests ProtectionManager.
 */
public class TestProtectionManager {
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  private final static Path FINAL_PATH = new Path("/test/testFinal/final/");
  private final static Path WHITE_PATH = new Path("/test/testWhite/white/");
  private static final Path CURRENT = new Path("Current");

  @Before
  public void setUp() throws IOException{
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(ProtectionManager.PROTECT_DATA_ENABLE_KEY, true);
    conf.set(ProtectionManager.FINAL_PATHS_KEY, FINAL_PATH.toString());
    conf.set(ProtectionManager.WHITE_PATHS_KEY, WHITE_PATH.toString());

    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws IOException{
    if(fs != null){
      fs.close();
    }
    if(cluster != null){
      cluster.shutdown();
    }
  }

  // creates a zero file.
  private void touchFile(Path path)
      throws IOException {
    fs.create(path).close();
  }


  // mkdir
  private void mkdir(Path dir)
      throws IOException {
    fs.mkdirs(dir);
  }

  private void assertCannotRename(Path src) {
    Path dst = new Path(src.toString() + Time.now());
    boolean result = false;
    boolean hitException = false;
    IOException ioe = null;

    try {
      result = fs.rename(src, dst);
    } catch (IOException e) {
      hitException = true;
      ioe = e;
    }

    assertTrue(!result);
    assertTrue(hitException);
    assertTrue(ioe instanceof AccessControlException);
  }

  private void assertCanRename(Path src) {
    Path dst = new Path(src.toString() + Time.now());
    boolean result = false;
    boolean hitException = false;
    IOException ioe = null;

    try {
      result = fs.rename(src, dst);
    } catch (IOException e) {
      hitException = true;
      ioe = e;
    }

    assertTrue(result);
    assertTrue(!hitException);
    assertTrue(ioe == null);
  }

  private void assertCannotDelete(Path src) {
    boolean result = false;
    boolean hitException = false;
    IOException ioe = null;

    try {
      result = fs.delete(src, true);
    } catch (IOException e) {
      hitException = true;
      ioe = e;
    }

    assertTrue(!result);
    assertTrue(hitException);
    assertTrue(ioe instanceof AccessControlException);
  }

  private void assertCanDelete(Path src) {
    boolean result = false;
    boolean hitException = false;
    IOException ioe = null;

    try {
      result = fs.delete(src, true);
    } catch (IOException e) {
      hitException = true;
      ioe = e;
    }

    assertTrue(result);
    assertTrue(!hitException);
    assertTrue(ioe == null);
  }

  private void assertCannotOverwrite(Path src) {
    FSDataOutputStream result = null;
    boolean hitException = false;
    IOException ioe = null;

    try {
      result = fs.create(src);
    } catch (IOException e) {
      hitException = true;
      ioe = e;
    }

    assertTrue(result == null);
    assertTrue(hitException);
    assertTrue(ioe instanceof AccessControlException);
  }

  private void assertCanOverwrite(Path src) {
    FSDataOutputStream result = null;
    boolean hitException = false;
    IOException ioe = null;

    try {
      result = fs.create(src);
    } catch (IOException e) {
      hitException = true;
      ioe = e;
    }

    assertTrue(result != null);
    assertTrue(!hitException);
    assertTrue(ioe == null);
  }

  private void assertTrulyDelete(Path src) throws IOException {
    fs.delete(src, true);

    Path trashRoot = fs.getTrashRoot(src);
    Path trashPath = new Path(trashRoot, src);
    assertTrue(!fs.exists(trashPath));
  }

  private void assertFakeDelete(Path src) throws IOException {
    fs.delete(src, true);

    Path trashRoot = fs.getTrashRoot(src);
    Path trashCurrent = new Path(trashRoot, CURRENT);
    Path trashPath = Path.mergePaths(trashCurrent, src);
    assertTrue(fs.exists(trashPath));
  }

  /**
   * Test FinalPath rules.
   */
  @Test
  public void testFinalPath() throws IOException {
    Path src;
    mkdir(FINAL_PATH);

    assertCannotDelete(FINAL_PATH);
    assertCannotRename(FINAL_PATH);

    src = new Path(FINAL_PATH, "a.txt");
    touchFile(src);
    assertCannotDelete(src);
    assertCannotRename(src);
    assertCannotOverwrite(src);

    src = new Path(FINAL_PATH, "dir/");
    mkdir(src);
    assertCannotDelete(src);
    assertCannotRename(src);

    src = new Path(FINAL_PATH, "dir/a.txt");
    touchFile(src);
    assertCanDelete(src);
    touchFile(src);
    assertCanRename(src);
    touchFile(src);
    assertCanOverwrite(src);

    src = new Path(FINAL_PATH, "dir1/dir2/");
    mkdir(src);
    assertCanDelete(src);
    mkdir(src);
    assertCanRename(src);

    src = new Path(FINAL_PATH, "dir1/dir2/a.txt");
    touchFile(src);
    assertCanDelete(src);
    touchFile(src);
    assertCanRename(src);
    touchFile(src);
    assertCanOverwrite(src);

    src = new Path("/test/testFinal/");
    assertCannotDelete(src);
    assertCannotRename(src);

    src = new Path("/test/");
    assertCannotDelete(src);
    assertCannotRename(src);

    src = new Path("/");
    assertCannotDelete(src);
    assertCannotRename(src);

    src = new Path("/test/testFinal/a.txt");
    touchFile(src);
    assertCanDelete(src);
    touchFile(src);
    assertCanRename(src);
    touchFile(src);
    assertCanOverwrite(src);
  }

  /**
   * Test WhitePath rules.
   */
  @Test
  public void testWhitePath() throws IOException {
    Path src;

    mkdir(WHITE_PATH);
    assertTrulyDelete(WHITE_PATH);

    src = new Path(WHITE_PATH, "a.txt");
    touchFile(src);
    assertTrulyDelete(src);

    src = new Path(WHITE_PATH, "dir/");
    mkdir(src);
    assertTrulyDelete(src);

    src = new Path(WHITE_PATH, "dir/a.txt");
    touchFile(src);
    assertTrulyDelete(src);

    src = new Path(WHITE_PATH, "dir1/dir2/");
    mkdir(src);
    assertTrulyDelete(src);

    src = new Path("/test/testWhite/");
    assertFakeDelete(src);

    src = new Path("/test/testWhite/a.txt");
    touchFile(src);
    assertFakeDelete(src);

    src = new Path("/test/a.txt");
    touchFile(src);
    assertFakeDelete(src);

    src = new Path("/test/double_delete.txt");
    touchFile(src);
    assertFakeDelete(src);
    touchFile(src);
    assertFakeDelete(src);
  }

  /**
   * Test Trash rules.
   */
  @Test
  public void testTrash() throws IOException {
    Path trashRoot = fs.getTrashRoot(new Path("/"));
    mkdir(trashRoot);
    assertCannotDelete(trashRoot);

    Path parent = trashRoot.getParent();
    assertCannotDelete(parent);

    Path ancestor = parent.getParent();
    assertCannotDelete(ancestor);
  }

  /**
   * Test WhiteIP rules.
   */
  @Test
  public void testWhiteIP() throws IOException {
    Path trashRoot = fs.getTrashRoot(new Path("/"));
    Path path = new Path(trashRoot, "dir/a.txt");
    touchFile(path);
    assertTrulyDelete(path);
  }
}
