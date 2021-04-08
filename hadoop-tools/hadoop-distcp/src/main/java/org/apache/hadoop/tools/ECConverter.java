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

package org.apache.hadoop.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.mapred.CopyMapper;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Files;


/**
 * DistCp is the main driver-class for DistCpV2.
 * For command-line use, DistCp::main() orchestrates the parsing of command-line
 * parameters and the launch of the DistCp job.
 * For programmatic use, a DistCp object can be constructed by specifying
 * options (in a DistCpOptions object), and DistCp::execute() may be used to
 * launch the copy-job. DistCp may alternatively be sub-classed to fine-tune
 * behaviour.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ECConverter extends DistCp {
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
    Configuration.addDefaultResource("yarn-default.xml");
    Configuration.addDefaultResource("yarn-site.xml");
    Configuration.addDefaultResource("distcp-default.xml");
    Configuration.addDefaultResource("ecconverter-site.xml");
  }

  static final Logger LOG = LoggerFactory.getLogger(ECConverter.class);

  // some configurations
  public static final String DFS_EC_CONVERT_PATHS = "dfs.ec.convert.paths";

  public static final String DFS_EC_CONVERT_POLICY = "dfs.ec.convert.policy";
  public static final String DFS_EC_CONVERT_POLICY_DEFAULT = "RS-6-3-1024k";

  public static final String DFS_EC_CONVERT_MTIME_THRESHOLD_DAYS =
      "dfs.ec.convert.mtime.threshold.days";
  public static final long DFS_EC_CONVERT_MTIME_THRESHOLD_DAYS_DEFAULT = 30;

  public static final String DFS_EC_CONVERT_ATIME_THRESHOLD_DAYS =
      "dfs.ec.convert.atime.threshold.days";
  public static final long DFS_EC_CONVERT_ATIME_THRESHOLD_DAYS_DEFAULT = 0;

  public static final String DFS_EC_CONVERT_MAX_MAP_TASKS =
      "dfs.ec.convert.max.map.tasks";
  public static final int DFS_EC_CONVERT_MAX_MAP_TASKS_DEFAULT  = 20;

  public static final String DFS_EC_CONVERT_SKIPCRC = "dfs.ec.convert.skipcrc";
  public static final boolean DFS_EC_CONVERT_SKIPCRC_DEFAULT = false;

  public static final String DFS_EC_CONVERT_INTERVAL_MINUTES =
      "dfs.ec.convert.interval.minutes";
  public static final long DFS_EC_CONVERT_INTERVAL_MINUTES_DEFAULT = 60;

  // internal dirs and log files
  private static final Path DFS_EC_CONVERT_TMP_DIR =
      new Path("/tmp/ecconverter/");
  private static final Path EC_CONVERT_LOG_PATH = new Path("/logs");
  private static final Path EC_CONVERT_WORKING_PATH = new Path("/working");
  private static final Path DFS_EC_CONVERT_ID_PATH =
      new Path("/system/ecconverter.id");

  private Configuration conf;
  private String ecPolicy;
  private Path[] ecPaths = new Path[0];;
  private long sleepIntervalMills;
  private boolean skipCrcCheck;
  private int ecMaxTasks;
  private final Timer ecWorkTimer;
  private TimerTask timerTask;
  private final List<Path> markedRunningClusters = new ArrayList<Path>();

  ECConverter() {
    this.ecWorkTimer = new Timer("ECConverterTimer", true);
  }

  private Path getEcTmpDir(Path ecPath) throws IOException {
    FileSystem fs = ecPath.getFileSystem(conf);
    Path ecTmpDir = Path.mergePaths(new Path(fs.getUri()),
       DFS_EC_CONVERT_TMP_DIR);
    return ecTmpDir;
  }

  private Path getEcLogDir(Path ecPath) throws IOException {
    FileSystem fs = ecPath.getFileSystem(conf);
    Path ecTmpDir = Path.mergePaths(new Path(fs.getUri()),
        DFS_EC_CONVERT_TMP_DIR);
    Path ecLogDir = Path.mergePaths(ecTmpDir, EC_CONVERT_LOG_PATH);
    return ecLogDir;
  }

  private Path getEcWorkingDir(Path ecPath) throws IOException {
    FileSystem fs = ecPath.getFileSystem(conf);
    Path ecTmpDir = Path.mergePaths(new Path(fs.getUri()),
        DFS_EC_CONVERT_TMP_DIR);
    Path ecWorkingDir = Path.mergePaths(ecTmpDir, EC_CONVERT_WORKING_PATH);
    return ecWorkingDir;
  }

  private Path getEcIdPath(Path ecPath) throws IOException {
    URI fsUri = ecPath.getFileSystem(conf).getUri();
    Path ecIdPath = Path.mergePaths(new Path(fsUri), DFS_EC_CONVERT_ID_PATH);
    return ecIdPath;
  }

  private String[] prepareDistCpArgs(Path ecPath) throws IOException {
    List<String> args = new ArrayList<>();
    // direct copy
    args.add("-direct");

    // skip crc
    if (skipCrcCheck) {
      args.add("-skipcrccheck");
    } else {
      // use COMPOSITE_CRC to compare crc between replication and ec files
      conf.set("dfs.checksum.combine.mode", "COMPOSITE_CRC");
    }

    // max tasks
    args.add("-m");
    args.add(String.valueOf(ecMaxTasks));

    // log dir
    Path ecLogDir = getEcLogDir(ecPath);
    args.add("-v");
    args.add("-log");
    args.add(ecLogDir.toString());

    // distcp source: intended ec paths
    args.add(ecPath.toString());

    // distcp target: ec tmp dir
    Path ecWorkingDir = getEcWorkingDir(ecPath);
    args.add(ecWorkingDir.toString());

    return args.toArray(new String[0]);
  }

  /**
   * filter out these files not to convert:
   * 1. already-ec files.
   * 2. opening-for-write files
   * 3. not old enough files
   */
  static class ECEonverterCopyFilter extends CopyFilter {
    private Configuration conf;
    public ECEonverterCopyFilter(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public boolean shouldCopy(Path path) {
      FileSystem fs;
      FileStatus status;

      try {
        fs = path.getFileSystem(conf);
        Path absPath = Path.getPathWithoutSchemeAndAuthority(path);
        status = fs.getFileStatus(absPath);

        // directory always false
        if (status.isDirectory()) {
          return false;
        }

        // filter out already-ec files
        if (status.isErasureCoded()) {
          return false;
        }

        // filter out opening-for-write files
        DFSClient dfsClient = ((DistributedFileSystem)fs).getClient();
        LocatedBlocks lbs = dfsClient
            .getLocatedBlocks(absPath.toString(), 0L, 1L);
        if (lbs.isUnderConstruction()) {
          return false;
        }

        // filter mtime
        long ecThresholdMtimeMills = TimeUnit.DAYS.toMillis(
            conf.getLong(DFS_EC_CONVERT_MTIME_THRESHOLD_DAYS,
                DFS_EC_CONVERT_MTIME_THRESHOLD_DAYS_DEFAULT));
        if (status.getModificationTime() + ecThresholdMtimeMills
            > System.currentTimeMillis()) {
          return false;
        }

        // filter atime
        long ecThresholdAtimeMills = TimeUnit.DAYS.toMillis(
            conf.getLong(DFS_EC_CONVERT_ATIME_THRESHOLD_DAYS,
                DFS_EC_CONVERT_ATIME_THRESHOLD_DAYS_DEFAULT));
        if (status.getAccessTime() + ecThresholdAtimeMills
            > System.currentTimeMillis()) {
          return false;
        }
      } catch (IOException e) {
        LOG.warn("ECCopyFilter meets exception for path " + path, e);
        return false;
      }
      return true;
    }
  }

  /*
   * ECConverter mapper, a 4-stages working:
   * 1. copy using distcp.
   * 2. rename to original position.
   * 3. preserve original file attributes.
   * 4. write renaming log
   */
  static class ECConverterMapper extends CopyMapper {
    @Override
    public void map(Text relPath, CopyListingFileStatus sourceFileStatus,
        Mapper<Text, CopyListingFileStatus, Text, Text>.Context context)
            throws IOException, InterruptedException {
      // omit directory
      if (sourceFileStatus.isDirectory()) {
        return;
      }

      // copy to tmp ec directory
      super.map(relPath, sourceFileStatus, context);

      // manually retrieve all original file attributes
      Configuration conf = context.getConfiguration();
      Path sourcePath = sourceFileStatus.getPath();
      DistributedFileSystem dfs =
          (DistributedFileSystem)(sourcePath.getFileSystem(conf));
      FileStatus status = dfs.getFileStatus(sourcePath);
      boolean preserveAcls = status.hasAcl();
      CopyListingFileStatus originalSourceStatus =
          DistCpUtils.toCopyListingFileStatusHelper(dfs,
            status,
            preserveAcls,
            true, true,
            sourceFileStatus.getChunkOffset(),
            sourceFileStatus.getChunkLength());

      // rename to origin
      Path targetWorkPath =
          new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
      Path target = new Path(targetWorkPath.makeQualified(dfs.getUri(),
          dfs.getWorkingDirectory()) + relPath.toString());
      dfs.rename(target, sourcePath, Rename.OVERWRITE);

      // preserve all original file attributes
      EnumSet<FileAttribute> attrs = EnumSet.allOf(FileAttribute.class);
      if (!preserveAcls) {
        attrs.remove(FileAttribute.ACL);
      }
      DistCpUtils.preserve(dfs, sourcePath, originalSourceStatus, attrs, true);

      // write log
      String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")
          .format(System.currentTimeMillis());
      context.write(null,
          new Text("FILE_RENAMED: source=" + target +
              " --> target=" + sourcePath + ", date: " + date));
    }
  }

  /**
   * delete ec converter dirs
   */
  static class ECConverterCleanup implements Runnable {
    private ECConverter converter;
    public ECConverterCleanup(ECConverter converter) {
      this.converter = converter;
    }

    @Override
    public void run() {
      converter.cleanupECConverterDirs();
    }
  }

  /**
   * prepare ec tmp/log/working dirs
   */
  private void prepareECConverterDirs(Path ecPath) throws IOException {
    try {
      FileSystem fs = ecPath.getFileSystem(conf);
      Path ecTmpDir = getEcTmpDir(ecPath);
      Path ecLogDir = getEcLogDir(ecPath);
      Path ecWorkingDir = getEcWorkingDir(ecPath);

      // delete corresponding dirs and create them again
      fs.delete(ecTmpDir, true);
      fs.mkdirs(ecTmpDir);
      fs.mkdirs(ecLogDir);
      fs.mkdirs(ecWorkingDir);

      // set ec-working dir's policy
      ((DistributedFileSystem)fs).enableErasureCodingPolicy(ecPolicy);
      ((DistributedFileSystem)fs).setErasureCodingPolicy(
          ecWorkingDir, ecPolicy);
    } catch (IOException e) {
      LOG.error("Unable to prepare ecEconverter dirs for " + ecPath, e);
      throw e;
    }
  }

  /*
   * delete ec tmp/log/working dir
   */
  private void cleanupECConverterDirs() {
    for (Path ecPath : ecPaths) {
      cleanupECConverterDir(ecPath);
    }
  }

  private void cleanupECConverterDir(Path ecPath) {
    Path ecTmpDir = null;
    try {
      ecTmpDir = getEcTmpDir(ecPath);
      ecTmpDir.getFileSystem(conf).delete(ecTmpDir, true);
    } catch (IOException e) {
      LOG.error("Unable to cleanup ecEconverter tmp dir: " + ecTmpDir, e);
    }
  }

  /**
   * customize distcp
   * 1. alter mapper to ECConverterMapper
   * 2. alter filter to ECConverterCopyFilter
   */
  private void customizeDistcp() {
    // mapper
    conf.setClass(DistCpConstants.CONF_LABEL_MAPPER_CLASS,
        ECConverterMapper.class,
        Mapper.class);

    // filter
    conf.setClass(DistCpConstants.CONF_LABEL_FILTERS_CLASS,
        ECEonverterCopyFilter.class,
        CopyFilter.class);
  }

  /*
   * write converted files during last distcp to local log file.
   */
  private void writeConvertedFilesLog(Path ecPath)
      throws IOException {
    try {
      Path ecLogDir = getEcLogDir(ecPath);
      FileSystem fs = ecLogDir.getFileSystem(conf);
      FileStatus[] status = fs.listStatus(ecLogDir);
      for (FileStatus s : status) {
        BufferedReader br = new BufferedReader(
            new InputStreamReader(fs.open(s.getPath())));
        for (String line = br.readLine(); line != null; line = br.readLine()) {
          LOG.info(line);
        }
        br.close();
      }
    } catch (IOException e) {
      LOG.error("Write converted file lists to log meets exception ", e);
      throw e;
    }
  }

  /**
   * a 6-stages procedure:
   * 1. prepare ec converter dirs with right ec policy.
   * 2. prepare distcp args
   * 3. run distcp
   * 4. write converted file lists to local log file.
   * 5. run distcp.cleanup
   * 6. delete ec converter dirs.
   */
  private void doConvert() {
    long startTime = System.currentTimeMillis();

    // refresh conf
    conf = new Configuration();
    setConf(conf);

    // validate convert config
    try {
      validateConvertConfig();
    } catch (IOException e) {
      LOG.warn("ECconverter validate config meets exception ", e);
      return;
    }

    // traverse ec paths one by one
    for (Path ecPath : ecPaths) {
      try {
        // check and mark running
        checkAndMarkRunning(ecPath);

        // prepare convert dirs
        prepareECConverterDirs(ecPath);

        // get distcp args
        String[] distcpArgs = prepareDistCpArgs(ecPath);

        // run distcp
        if (super.run(distcpArgs) == DistCpConstants.SUCCESS) {
          // write converted file list
          writeConvertedFilesLog(ecPath);
        }

        // clean distcp
        super.cleanup();

        // clean ec convert dirs
        cleanupECConverterDir(ecPath);
      } catch (Exception e) {
        LOG.warn("ECconverter meets exception while converting " + ecPath, e);
      }
    }

    // print consumed time and corresponding paths
    long elasped = System.currentTimeMillis() - startTime;
    StringBuilder pathsMessage = new StringBuilder();
    for (Path d : ecPaths) {
      pathsMessage.append(d.toString()).append(" ");
    }
    LOG.info("Took " +  elasped/1000 + " seconds to convert " + pathsMessage);
  }

  /**
   * make sure that only one converting instance running for an
   * individual ec path.
   */
  private void checkAndMarkRunning(Path ecPath) throws IOException {
    Path ecId = getEcIdPath(ecPath);
    // mark one cluster only once
    if (markedRunningClusters.contains(ecId)) {
      return;
    }

    FileSystem fs = ecId.getFileSystem(conf);
    try {
      if (fs.exists(ecId)) {
        // try appending to it so that it will fail fast if another balancer is
        // running.
        IOUtils.closeStream(fs.append(ecId));
        fs.delete(ecId, true);
      }
      FSDataOutputStream fsout = fs.create(ecId, false);
      fs.deleteOnExit(ecId);
      fsout.writeBytes(InetAddress.getLocalHost().getHostName());
      fsout.writeBytes(System.lineSeparator());
      fsout.hflush();

      // marked converting
      markedRunningClusters.add(ecId);
    } catch(RemoteException e) {
      if (AlreadyBeingCreatedException.class.getName()
          .equals(e.getClassName())) {
        Path absIdPath = ecId.makeQualified(fs.getUri(), ecId);
        LOG.error("Another ECConverter is running, please check id file "
              + "to visit it: " + absIdPath);
      }
      throw e;
    } catch (IOException e) {
        LOG.error("ECConverter checkAndMarkRunning meets exception", e);
        throw e;
    }
  }

  /**
   * Because there are possible many ec paths, allow them to be indirected
   * through a file by specifying the configuration as "@/path/to/local/file".
   * If this syntax is used, this function will return the contents of the file
   * as a String[].
   */
  public String[] resolveEcPathsIndirection()
      throws IOException {
    LinkedList<String> ret = new LinkedList<>();
    String[] ecPaths = conf.getTrimmedStrings(DFS_EC_CONVERT_PATHS);
    for (String p : ecPaths) {
      // direct HDFS path
      if (!p.startsWith("@")) {
        ret.add(p);
        continue;
      }

      // indirect local FileSystem file
      String localFile = p.substring(1).trim();
      File file = new File(localFile);
      if (file.exists()) {
        String content =
            Files.asCharSource(file, Charsets.UTF_8).read().trim();
        ret.addAll(Arrays.asList(content.split("\\n+")));
      } else {
        LOG.warn("Local ec paths file " + file + " does not exist, ingore it!");
      }
    }
    return ret.toArray(new String[0]);
  }

  /**
   * validate user config, now just check src ec paths
   */
  private void validateConvertConfig() throws IOException {
    // customize distcp
    customizeDistcp();

    // get ec policy, crc, max tasks etc.
    ecPolicy = conf.getTrimmed(
        DFS_EC_CONVERT_POLICY, DFS_EC_CONVERT_POLICY_DEFAULT);
    skipCrcCheck = conf.getBoolean(
        DFS_EC_CONVERT_SKIPCRC, DFS_EC_CONVERT_SKIPCRC_DEFAULT);
    ecMaxTasks = conf.getInt(
        DFS_EC_CONVERT_MAX_MAP_TASKS, DFS_EC_CONVERT_MAX_MAP_TASKS_DEFAULT);

    // refresh convert timer if necessary
    long sleepInterval = TimeUnit.MINUTES.toMillis(conf.getLong(
        DFS_EC_CONVERT_INTERVAL_MINUTES,
        DFS_EC_CONVERT_INTERVAL_MINUTES_DEFAULT));
    if (this.sleepIntervalMills != sleepInterval) {
      // update sleep interval
      this.sleepIntervalMills = sleepInterval;

      // cancel old timer task
      if (timerTask != null) {
        timerTask.cancel();
      }

      // create a new one
      timerTask = new TimerTask() {
        @Override
        public void run() {
          doConvert();
        }
      };
      ecWorkTimer.schedule(timerTask, sleepInterval, sleepInterval);
    }

    // validate src ec paths
    String[] srcEcPaths = resolveEcPathsIndirection();
    List<Path> list = new LinkedList<Path>();
    for (int i = 0; i < srcEcPaths.length; i++) {
      Path p = new Path(srcEcPaths[i]);
      try {
        FileSystem fs = p.getFileSystem(conf);
        Path absPath = p.makeQualified(fs.getUri(), p);

        // ECconverter only support HDFS
        if (!fs.getScheme().equalsIgnoreCase(HdfsConstants.HDFS_URI_SCHEME)) {
          LOG.warn("ECConverter only support HDFS, but " + absPath
              + " is not a HDFS path, ingore it.");
          continue;
        }

        // src ec path must exist
        if (!fs.exists(p)) {
          LOG.warn("Source ec path " + absPath + " does not exist, ignore it.");
          continue;
        }

        // present absolute paths
        list.add(absPath);
      } catch (Exception e) {
        LOG.warn("Validate ec path meets exception for " + p, e);
      }
    }

    // uniq ec paths
    List<Path> uniqPath =
        list.stream().distinct().collect(Collectors.toList());
    this.ecPaths = uniqPath.toArray(new Path[0]);
    if (this.ecPaths.length == 0) {
      throw new IOException("No ec paths exist!");
    }
  }

  @Override
  public int run(String[] argv) {
    // initialize convert task
    timerTask = new TimerTask() {
      @Override
      public void run() {
        doConvert();
      }
    };

    // trigger convert job
    ecWorkTimer.schedule(timerTask, 0);

    // infinite looping
    while (true) {
      try {
        Thread.sleep(sleepIntervalMills);
      } catch (InterruptedException e) {}
    }
  }

  /**
   * Main function of the ECConverter program.
   */
  public static void main(String argv[]) {
    StringUtils.startupShutdownMessage(ECConverter.class, argv, LOG);

    try {
      ECConverter converter = new ECConverter();
      ECConverterCleanup clean = new ECConverterCleanup(converter);

      ShutdownHookManager.get().addShutdownHook(clean,
          SHUTDOWN_HOOK_PRIORITY);
      ToolRunner.run(converter, argv);
    }
    catch (Exception e) {
      LOG.error("Couldn't complete ECConverter operation: ", e);
    }
  }
}
