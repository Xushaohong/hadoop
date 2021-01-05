/*
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
/**
 * Provide data protect functionalities, right now there are 4 type of rules:
 *
 * FinalPath rules:
 *  1. a finalPath itself can't be deleted or renamed.
 *  2. a finalPaths's parent directory and ancestor directory can't be
 *     deleted or renamed.
 *  3. if a finalPath refers to a directory, then its children can't be
 *     deleted or renamed, note this is not recursive.
 *
 * Trash rules:
 *  1. Trash directory itself and its parent or ancestor can't be deleted.
 *
 * WhiteIPs rules:
 *  1. only a whiteIP can delete trash's contents.
 *
 * WhitePath rules:
 *  1. a whitePath itself can be deleted.
 *  2. if a whitePath refers to a directory, then its children can also be
 *     deleted, note this is recursive.
 *  3. delete a non-white path results in MoveToTrash.
 *
 * In addition, these 3 keys can configured either direction or indirection:
 *  1. hadoop.tq.final.path
 *  2. hadoop.tq.final.path
 *  3. hadoop.tq.white.ipaddr
 *
 *  e.g. We can write hadoop.tq.white.path in either of the two patterns below:
 *    <property>
 *      <name>hadoop.tq.white.path</name>
 *      <value>/whiteTest/white;/dir1;</value>
 *    </property>
 *
 *    or:
 *
 *    <property>
 *      <name>hadoop.tq.white.path</name>
 *      <value>@/root/whitePaths.txt</value>
 *    </property>
 *
 *  The file /root/whitePaths.txt contains the real white paths (one path per
 *  line).
 */
public class ProtectionManager {
  private static final Log LOG = LogFactory.getLog(ProtectionManager.class);

  public static final String PROTECT_DATA_ENABLE_KEY =
                                        "hadoop.tq.protect.data.enable";
  public static final boolean PROTECT_DATA_ENABLE_DEFAULT = false;
  public static final String FINAL_PATHS_KEY = "hadoop.tq.final.path";
  public static final String WHITE_PATHS_KEY = "hadoop.tq.white.path";
  public static final String WHITE_IPS_KEY = "hadoop.tq.white.ipaddr";

  private static final Path CURRENT = new Path("Current");
  private static final Path TRASH = new Path(FileSystem.TRASH_PREFIX);
  private static final FsPermission PERMISSION =
    new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  private NameNodeRpcServer nameNodeRpcServer;
  private AtomicReference<Configuration> conf = new AtomicReference<>();

  private AtomicBoolean isProtectDataEnabled = new AtomicBoolean();
  private AtomicBoolean isHDFSEncryptionEnabled = new AtomicBoolean();
  private AtomicReference<List<String>> finalPaths = new AtomicReference<>();
  private AtomicReference<List<String>> whitePaths = new AtomicReference<>();
  private AtomicReference<List<String>> whiteIPs = new AtomicReference<>();

  public ProtectionManager(final NameNodeRpcServer server,
      final Configuration conf) throws IOException {
    this.nameNodeRpcServer = server;
    initialize(conf);
  }

  private void initialize(Configuration config) throws IOException {
    conf.set(config);
    isProtectDataEnabled.set(config.getBoolean(PROTECT_DATA_ENABLE_KEY,
        PROTECT_DATA_ENABLE_DEFAULT));
    isHDFSEncryptionEnabled.set(!config.getTrimmed(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH, "")
        .isEmpty());
    finalPaths.set(parseFinalPaths());
    whitePaths.set(parseWhitePaths());
    whiteIPs.set(parseWhiteIPs());
  }

  private List<String> parseFinalPaths() throws IOException {
    return parsePaths(FINAL_PATHS_KEY);
  }

  private List<String> parseWhitePaths() throws IOException {
    return parsePaths(WHITE_PATHS_KEY);
  }

  private List<String> parsePaths(String key) throws IOException {
    List<String> paths = new ArrayList<String>();
    String value = resolveConfIndirection(key);
    if (value != null) {
      String[] fixedValues = value.split(";|\n");
      for (String v : fixedValues) {
        // processing ".",  "..", "//", trailing "/" etc.
        String path = new Path(v).toString();
        if (!paths.contains(path)) {
          paths.add(path);
        }
      }
    }
    return paths;
  }

  private List<String> parseWhiteIPs() throws IOException {
    List<String> ips = new ArrayList<String>();
    // add NameNode RpcServer's IP address
    ips.add(nameNodeRpcServer.getRpcAddress().getAddress().getHostAddress());

    String value = resolveConfIndirection(WHITE_IPS_KEY);
    if (value != null) {
      String[] fixedIPs= value.split(";|\n");
      for (String ip : fixedIPs) {
        if (!ips.contains(ip)) {
          ips.add(ip);
        }
      }
    }
    return ips;
  }

  private String resolveConfIndirection(String key)
      throws IOException {
    String valInConf = conf.get().get(key);
    if (valInConf == null) {
      return null;
    }

    if (!valInConf.startsWith("@")) {
      return valInConf;
    }

    String path = valInConf.substring(1).trim();
    return Files.toString(new File(path), Charsets.UTF_8).trim();
  }

  public void refreshProtection(Configuration newConfig) throws IOException {
    initialize(newConfig);
  }

  public String getProtection() throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         PrintStream ps = new PrintStream(baos)) {
      // write isProtectDataEnabled flag
      ps.println("isProtectDataEnabled:");
      ps.println(isProtectDataEnabled);
      ps.println();

      // write final paths
      ps.println("Final Paths:");
      for (String path : finalPaths.get()) {
        ps.println(path);
      }
      ps.println();

      // write white paths
      ps.println("White Paths:");
      for (String path : whitePaths.get()) {
        ps.println(path);
      }
      ps.println();

      // write white ips
      ps.println("White IPs:");
      for (String ip : whiteIPs.get()) {
        ps.println(ip);
      }
      ps.println();

      return baos.toString();
    }
  }

  /**
   * Check if the former path is the child of the latter path
   * isChild("/aa/bb", "/aa") returns true.
   * isChild("/aa/bb/cc.txt", "/aa") returns true.
   * isChild("/aaa/bb", "/aa") returns false.
   * isChild("/aaa/bb", "/") returns true.
   */
  private boolean isChild(String child, String path) {
    if (child.length() > path.length()
        && child.startsWith(path)
        && child.charAt(path.length()) == Path.SEPARATOR_CHAR) {
      return true;
    }
    return false;
  }

  /**
   * Check finalPath protection.
   *
   * @throws AccessControlException if the src encounter a violation against
   *         any finalPath.
   */
  private void checkFinalPath(String src) throws IOException {
    String parent = new Path(src).getParent().toString();

    for (String finalPath : finalPaths.get()) {
      if (finalPath.equals(src)
          || finalPath.equals(parent)
          || isChild(finalPath, src)) {
        throw new AccessControlException(src + " cannot be modified due to a "
            + "violation against finalPath: " + finalPath);
      }
    }
  }

  /**
   * Check whitePath protection.
   *
   * @param src the path name
   *
   * @return true if the src is within the effective scope of any whilePath,
   *         otherwise return false.
   */
  private boolean checkWhitePath(String src) throws IOException {
    for (String whitePath : whitePaths.get()) {
      if (whitePath.equals(src) || isChild(src, whitePath)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check whiteIP protection.
   */
  private void checkWhiteIP(String src) throws IOException {
    String ip = Server.getRemoteAddress();
    if (!whiteIPs.get().contains(ip)) {
      throw new AccessControlException(ip + " cannot delete " + src +
          " because only whiteIPs can delete trash contents.");
    }
  }

  /**
   * Check Trash protection.
   */
  private void checkTrash(String src) throws IOException {
    String trashRoot = getTrashRoot(src);
    if (trashRoot.equals(src) || isChild(trashRoot, src)) {
      throw new AccessControlException(src + " cannot be deleted because it "
        + "is the parent or ancestor of Trash: " + trashRoot);
    }
  }

  public boolean rename(String src, String dst) throws IOException {
    // root directory is a special case, just delegate it to NN
    if (!src.equals("/")
        && isProtectDataEnabled.get()
        && nameNodeRpcServer.getFileInfo(src) != null) {
      checkFinalPath(src);
    }

    return nameNodeRpcServer.renameOriginal(src, dst);
  }

  public void rename2(String src, String dst, Options.Rename... options)
      throws IOException {
    // root directory is a special case, just delegate it to NN
    if (!src.equals("/")
        && isProtectDataEnabled.get()
        && nameNodeRpcServer.getFileInfo(src) != null) {
      checkFinalPath(src);
    }

    nameNodeRpcServer.rename2Original(src, dst, options);
  }

  public boolean delete(String src, boolean recursive) throws IOException  {
    // root directory is a special case, just delegate it to NN
    if (!src.equals("/")
        && isProtectDataEnabled.get()
        && nameNodeRpcServer.getFileInfo(src) != null) {

      // check finalPath rules.
      checkFinalPath(src);

      // check Trash rules
      checkTrash(src);

      // check WhiteIP rules
      String trashRoot = getTrashRoot(src);
      if (isChild(src, trashRoot)) {
        checkWhiteIP(src);
        return nameNodeRpcServer.deleteOriginal(src, recursive);
      }

      // check whitePath rules.
      if (checkWhitePath(src)) {
        return nameNodeRpcServer.deleteOriginal(src, recursive);
      } else {
        return moveToTrashWithRetry(src);
      }
    }

    return nameNodeRpcServer.deleteOriginal(src, recursive);
  }

  private String getUserName() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    if (ugi != null) {
      return ugi.getUserName();
    } else {
      return System.getProperty("user.name");
    }
  }

  /**
   * Get the root directory of Trash for a path in HDFS.
   * 1. File in encryption zone returns /ez1/.Trash/username
   * 2. File not in encryption zone, or encountered exception when checking
   *    the encryption zone of the path, returns /user/username/.Trash
   * Caller appends either Current or checkpoint timestamp for trash destination
   * @param path the trash root of the path to be determined.
   * @return trash root
   */
  private String getTrashRoot(String p) throws IOException {
    String userName = getUserName();

    if (isHDFSEncryptionEnabled.get()) {
      Path path = new Path(p);
      String parentSrc = path.isRoot() ? path.toString()
                                       : path.getParent().toString();
      try {
        EncryptionZone ez = nameNodeRpcServer.getEZForPath(parentSrc);
        if (ez != null) {
          return new Path(new Path(ez.getPath(), TRASH), userName).toString();
        }
      } catch (IOException e) {
        LOG.warn("Exception in checking the encryption zone for the " +
            "path " + parentSrc + ". " + e.getMessage());
        throw e;
      }
    }

    return new Path("/user/" + userName, TRASH).toString();
  }

  // try twice, in case checkpoint between the mkdirs() & rename()
  private boolean moveToTrashWithRetry(String path) throws IOException {
    for (int i = 0; i < 2; i++) {
      try {
        moveToTrash(path);
        return true;
      } catch (IOException e) {
        if (i == 1) {
          throw e;
        }
      }
    }
    return false;
  }

  private void moveToTrash(String path) throws IOException {
    String trashRoot = getTrashRoot(path);
    String trashCurrent = new Path(trashRoot, CURRENT).toString();
    String trashPath = Path.mergePaths(new Path(trashCurrent), new Path(path)).
                       toString();
    String baseTrashPath = new Path(trashPath).getParent().toString();

    // create base trash directory if it did not exist right now.
    try {
      if (nameNodeRpcServer.getFileInfo(baseTrashPath) == null) {
        if (!nameNodeRpcServer.mkdirs(baseTrashPath, PERMISSION, true)) {
          throw new IOException();
        }
      }
    } catch (IOException e) {
      String msg = "Can't create(mkdir) trash directory: " + baseTrashPath ;
      LOG.warn(msg, e);
      throw new IOException(msg, e);
    }

    // move to trash
    try {
      // if the target path in Trash already exists, then append with
      // a current time in milliseconds.
      String orig = trashPath;
      while (nameNodeRpcServer.getFileInfo(trashPath) != null) {
        trashPath = new Path(orig + Time.now()).toString();
      }

      if (nameNodeRpcServer.renameOriginal(path, trashPath)) {
        return;
      } else {
        throw new IOException();
      }
    } catch (IOException e) {
      String msg = "Failed to rename " + path + " to " + trashPath;
      LOG.warn(msg, e);
      throw new IOException(msg, e);
    }
  }
}
