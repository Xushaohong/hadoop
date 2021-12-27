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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.mapred.CopyCommitter;
import org.apache.hadoop.tools.mapred.CopyOutputFormat;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
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
public class ECConverter extends Configured implements Tool {
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

  /**
   * Priority of the shutdown hook.
   */
  static final int SHUTDOWN_HOOK_PRIORITY = 30;

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

  public static final String DFS_EC_CONVERT_LENGTH_THRESHOLD_BYTES =
      "dfs.ec.convert.length.threshold.bytes";
  public static final long DFS_EC_CONVERT_LENGTH_THRESHOLD_BYTES_DEFAULT = 0;

  // Run several EC convert jobs for different directories, this config decides
  // the thread number in EC converter thread pool.
  public static final String DFS_EC_CONVERT_MAX_CONCURRNET_JOBS =
          "dfs.ec.convert.max.concurrent.jobs";
  public static final int DFS_EC_CONVERT_MAX_CONCURRNET_JOBS_DEFAULT  = 1;

  public static final String DFS_EC_CONVERT_NUM_LISTSTATUS_THREADS =
      "dfs.ec.convert.num.liststatus.threads";
  public static final int DFS_EC_CONVERT_NUM_LISTSTATUS_THREADS_DEFAULT = 1;

  // Map number for one EC converter job.
  public static final String DFS_EC_CONVERT_MAX_MAP_TASKS =
      "dfs.ec.convert.max.map.tasks";
  public static final int DFS_EC_CONVERT_MAX_MAP_TASKS_DEFAULT  = 20;

  public static final String DFS_EC_CONVERT_BLOCKS_PER_CHUNK =
      "dfs.ec.convert.blocks.per.chunk";
  public static final int DFS_EC_CONVERT_BLOCKS_PER_CHUNK_DEFAULT = 0;

  public static final String DFS_EC_CONVERT_SKIPCRC = "dfs.ec.convert.skipcrc";
  public static final boolean DFS_EC_CONVERT_SKIPCRC_DEFAULT = false;

  public static final String DFS_EC_CONVERT_INTERVAL_MINUTES =
      "dfs.ec.convert.interval.minutes";
  public static final long DFS_EC_CONVERT_INTERVAL_MINUTES_DEFAULT = 60;

  // internal dirs and log files
  private static final Path DFS_EC_CONVERT_ROOT_DIR =
      new Path("/tmp/ecconverter/");
  private static final Path DFS_EC_CONVERT_LOG_DIR = new Path("/logs");
  private static final Path DFS_EC_CONVERT_WORKING_DIR = new Path("/working");
  private static final String DFS_EC_CONVERT_ID_DIR = "/system";

  private Configuration conf;
  private String ecPolicy;
  private Path[] ecPaths = new Path[0];
  private long sleepIntervalMills;
  private boolean skipCrcCheck;
  private int ecMaxJobs;
  private int listStatusThreads;
  private int ecMaxTasks;
  private int blocksPerChunk;
  private final Timer ecWorkTimer;
  private TimerTask timerTask;

  ECConverter() {
    this.ecWorkTimer = new Timer("ECConverterTimer", true);
  }


  /**
   * filter out these files not to convert:
   * 1. already-ec files.
   * 2. opening-for-write files
   * 3. not old enough files
   */
  static class ECEonverterCopyFilter extends CopyFilter {
    private Configuration conf;
    private long lengthThreshold;
    private long mtimeThreshold;
    private long atimeThreshold;

    public ECEonverterCopyFilter(Configuration conf) {
      this.conf = conf;
      this.lengthThreshold = conf.getLong(
          DFS_EC_CONVERT_LENGTH_THRESHOLD_BYTES,
          DFS_EC_CONVERT_LENGTH_THRESHOLD_BYTES_DEFAULT);
      this.mtimeThreshold = TimeUnit.DAYS.toMillis(
          conf.getLong(DFS_EC_CONVERT_MTIME_THRESHOLD_DAYS,
              DFS_EC_CONVERT_MTIME_THRESHOLD_DAYS_DEFAULT));
      this.atimeThreshold = TimeUnit.DAYS.toMillis(
          conf.getLong(DFS_EC_CONVERT_ATIME_THRESHOLD_DAYS,
              DFS_EC_CONVERT_ATIME_THRESHOLD_DAYS_DEFAULT));
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
          LOG.debug("Skip copy, reason: is directory, path: " + path);
          return false;
        }

        // filter out already-ec files
        if (status.isErasureCoded()) {
          LOG.debug("Skip copy, reason: already erasure coded, path: " + path);
          return false;
        }

        // filter length
        if (status.getLen() < lengthThreshold) {
          LOG.debug("Skip copy, reason: length less than threshold "
                  + lengthThreshold + ", path: " + path);
          return false;
        }

        // filter mtime
        if (status.getModificationTime() + mtimeThreshold
            > System.currentTimeMillis()) {
          LOG.debug("Skip copy, reason: mtime doesn't match threshold "
                  + mtimeThreshold + ", path: " + path);
          return false;
        }

        // filter atime
        if (status.getAccessTime() + atimeThreshold
            > System.currentTimeMillis()) {
          LOG.debug("Skip copy, reason: atime doesn't match threshold "
                  + atimeThreshold + ", path: " + path);
          return false;
        }

        // filter out opening-for-write files
        DFSClient dfsClient = ((DistributedFileSystem)fs).getClient();
        LocatedBlocks lbs = dfsClient
            .getLocatedBlocks(absPath.toString(), 0L, 1L);
        if (lbs.isUnderConstruction()) {
          LOG.debug("Skip copy, file under construction, path: " + path);
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
   * ECConverter output format
   */
  static class ECConverterOutputFormat<K, V> extends CopyOutputFormat<K, V> {
   @Override
   public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
     return new ECConverterCopyCommitter(getOutputPath(context), context);
   }
  }

  /*
   * ECConverter copy commiter, a 3-stages commit working:
   * 1. rename to original position.
   * 2. preserve original file attributes.
   * 3. write renaming log
   */
  static class ECConverterCopyCommitter extends CopyCommitter {
    public static final String RENAMED_FILES_LOG = "renamedFiles.txt";
    private Configuration conf;

    public ECConverterCopyCommitter(Path outputPath, TaskAttemptContext context)
        throws IOException {
      super(outputPath, context);
      this.conf = context.getConfiguration();
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      // backup source listing file
      Path sourceListing =
          new Path(conf.get(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH));
      URI sourceListingURI = sourceListing.toUri();
      Path backupPath =
          Path.mergePaths(DFS_EC_CONVERT_ROOT_DIR, sourceListing);
      Path sourceListingBackup = new Path(sourceListingURI.getScheme(),
          sourceListingURI.getAuthority(), backupPath.toString());
      FileSystem clusterFS = sourceListing.getFileSystem(conf);
      FSDataInputStream in = clusterFS.open(sourceListing);
      FSDataOutputStream out = clusterFS.create(sourceListingBackup);
      IOUtils.copyBytes(in, out, conf, true);

      // super commit
      super.commitJob(jobContext);

      // rename and preserve
      Path targetRoot =
          new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
      DistributedFileSystem dfs =
          (DistributedFileSystem)targetRoot.getFileSystem(conf);
      SequenceFile.Reader sourceReader = new SequenceFile.Reader(
          conf, SequenceFile.Reader.file(sourceListingBackup));
      Path outputPath = getOutputPath();
      FSDataOutputStream renamedFiles =
          dfs.create(new Path(outputPath, RENAMED_FILES_LOG));

      try {
        CopyListingFileStatus srcFileStatus = new CopyListingFileStatus();
        Text srcRelPath = new Text();

        // Iterate over every source path that was copied.
        while (sourceReader.next(srcRelPath, srcFileStatus)) {
          // omit directory
          if (srcFileStatus.isDirectory()) {
            continue;
          }

          // only process last chunk file
          if (srcFileStatus.getChunkOffset() + srcFileStatus.getChunkLength() !=
              srcFileStatus.getLen()) {
            continue;
          }

          Path srcFile = srcFileStatus.getPath();
          Path targetFile =
              Path.mergePaths(targetRoot, new Path(srcRelPath.toString()));

          // sanity length and mtime check
          FileStatus status = dfs.getFileStatus(srcFile);
          if ((status.getLen() != srcFileStatus.getLen())
              || (status.getModificationTime() != srcFileStatus.getModificationTime())) {
            continue;
          }

          // manually retrieve all original file attributes
          boolean preserveAcls = status.hasAcl();
          CopyListingFileStatus originalSourceStatus =
              DistCpUtils.toCopyListingFileStatusHelper(dfs,
                status,
                preserveAcls,
                true, true,
                srcFileStatus.getChunkOffset(),
                srcFileStatus.getChunkLength());

          // rename to origin
          dfs.rename(targetFile, srcFile, Rename.OVERWRITE);

          // preserve all original file attributes
          EnumSet<FileAttribute> attrs = EnumSet.allOf(FileAttribute.class);
          if (!preserveAcls) {
            attrs.remove(FileAttribute.ACL);
          }
          DistCpUtils.preserve(dfs, srcFile, originalSourceStatus, attrs, true);

          // write log
          String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")
              .format(System.currentTimeMillis());
          renamedFiles.write(new String("FILE_RENAMED: source=" + targetFile +
                  " --> target=" + srcFile + ", date: " + date).getBytes());
          renamedFiles.write(System.lineSeparator().getBytes());
        }
      } finally {
        IOUtils.closeStream(sourceReader);
        IOUtils.closeStream(renamedFiles);
      }
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

  private class ECExecutor extends DistCp implements Runnable {
    private Path ecPath;

    private Path ecTmpDir;
    private Path ecLogDir;
    private Path ecWorkingDir;

    ECExecutor(Path ecPath) {
      setConf(conf);
      this.ecPath = ecPath;
    }

    private void init() throws IOException {
      this.ecTmpDir = getEcTmpDir(ecPath);
      this.ecLogDir = Path.mergePaths(this.ecTmpDir, DFS_EC_CONVERT_LOG_DIR);
      this.ecWorkingDir = Path.mergePaths(this.ecTmpDir, DFS_EC_CONVERT_WORKING_DIR);
    }

    /**
     * make sure that only one converting instance running for an
     * individual ec path.
     */
    private void checkAndMarkRunning() throws IOException {
      FileSystem fs = ecPath.getFileSystem(conf);
      Path ecIdPath = Path.mergePaths(
              new Path(fs.getUri()),
              new Path(DFS_EC_CONVERT_ID_DIR + getEcId(ecPath)));

      try {
        if (fs.exists(ecIdPath)) {
          // try appending to it so that it will fail fast if another balancer is
          // running.
          IOUtils.closeStream(fs.append(ecIdPath));
          fs.delete(ecIdPath, true);
        }
        FSDataOutputStream fsout = fs.create(ecIdPath, false);
        fs.deleteOnExit(ecIdPath);
        fsout.writeBytes(InetAddress.getLocalHost().getHostName());
        fsout.writeBytes(System.lineSeparator());
        fsout.writeBytes(ecPath.toString());
        fsout.writeBytes(System.lineSeparator());
        fsout.hflush();
      } catch(RemoteException e) {
        if (AlreadyBeingCreatedException.class.getName()
                .equals(e.getClassName())) {
          Path absIdPath = ecIdPath.makeQualified(fs.getUri(), ecIdPath);
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
     * prepare ec tmp/log/working dirs
     */
    private void prepareECConverterDirs() throws IOException {
      try {
        FileSystem fs = ecPath.getFileSystem(conf);

        // delete corresponding dirs and create them again
        fs.delete(ecTmpDir, true);
        fs.mkdirs(ecTmpDir);
        fs.mkdirs(ecLogDir);
        fs.mkdirs(ecWorkingDir);

        // set ec-working dir's policy
        ((DistributedFileSystem)fs).enableErasureCodingPolicy(ecPolicy);
        ((DistributedFileSystem)fs).setErasureCodingPolicy(ecWorkingDir, ecPolicy);
      } catch (IOException e) {
        LOG.error("Unable to prepare ecEconverter dirs for " + ecPath, e);
        throw e;
      }
    }

    private String[] prepareDistCpArgs() throws IOException {
      List<String> args = new ArrayList<>();
      // direct copy
      args.add("-direct");

      // sync folder in case retry in CopyMapper
      args.add("-update");

      // skip crc
      if (skipCrcCheck) {
        args.add("-skipcrccheck");
      } else {
        // use COMPOSITE_CRC to compare crc between replication and ec files
        conf.set("dfs.checksum.combine.mode", "COMPOSITE_CRC");
      }

      // listStatus threads
      args.add("-numListstatusThreads");
      args.add(String.valueOf(listStatusThreads));

      // max tasks
      args.add("-m");
      args.add(String.valueOf(ecMaxTasks));

      // blocks per chunk
      args.add("-blocksperchunk");
      args.add(String.valueOf(blocksPerChunk));

      // log dir
      args.add("-v");
      args.add("-log");
      args.add(ecLogDir.toString());

      // distcp source: intended ec paths
      args.add(ecPath.toString());

      // distcp target: ec tmp dir
      args.add(ecWorkingDir.toString());

      return args.toArray(new String[0]);
    }

    private void cleanupECConverterDir() {
      try {
        ecTmpDir.getFileSystem(conf).delete(ecTmpDir, true);
      } catch (IOException e) {
        LOG.error("Unable to cleanup ecEconverter tmp dir: " + ecTmpDir, e);
      }
    }

    /*
     * write converted files during last distcp to local log file.
     */
    private void writeConvertedFilesLog() throws IOException {
      try {
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

    @Override
    public void run() {
      LOG.info("Start to convert dir: " + ecPath.toString());
      try {
        init();

        // check and mark running
        checkAndMarkRunning();

        // prepare convert dirs
        prepareECConverterDirs();

        // get distcp args
        String[] distcpArgs = prepareDistCpArgs();

        if (super.run(distcpArgs) == DistCpConstants.SUCCESS) {
          // write converted file list
          writeConvertedFilesLog();
        }

        super.cleanup();

        // clean ec convert dirs
        cleanupECConverterDir();
      } catch (Exception e) {
        LOG.warn("ECconverter meets exception while converting " + ecPath, e);
      }
    }
  }

  private String getEcId(Path ecPath) {
    String pathName = ecPath.getName();
    int hashCode = ecPath.hashCode();
    return pathName + "_" + hashCode;
  }

  private Path getEcRootDir(Path ecPath) throws IOException {
    FileSystem fs = ecPath.getFileSystem(conf);
    Path ecRootDir = Path.mergePaths(new Path(fs.getUri()),
            DFS_EC_CONVERT_ROOT_DIR);
    return ecRootDir;
  }

  private Path getEcTmpDir(Path ecPath) throws IOException {
    Path ecTmpDir = Path.mergePaths(getEcRootDir(ecPath),
            new Path("/" + getEcId(ecPath)));
    return ecTmpDir;
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
    conf.setClass(DistCpConstants.CONF_LABEL_OUTPUT_FORMAT_CLASS,
            ECConverterOutputFormat.class,
            OutputFormat.class);

    // filter
    conf.setClass(DistCpConstants.CONF_LABEL_FILTERS_CLASS,
            ECEonverterCopyFilter.class,
            CopyFilter.class);
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

    ExecutorService executorService = Executors.newFixedThreadPool(ecMaxJobs);
    for (Path p : ecPaths) {
      LOG.info("Submitting dir: " + p.toString());
      ECExecutor executor = new ECExecutor(p);
      executorService.submit(executor);
    }

    executorService.shutdown();
    try {
      boolean terminated = false;
      while (!terminated) {
        terminated = executorService.awaitTermination(
                10000, TimeUnit.SECONDS);
      }
      LOG.info("All tasks done!");
    } catch (InterruptedException e) {
      LOG.warn("Exiting... Caused by InterruptedException.");
      executorService.shutdownNow();
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
    ecMaxJobs = conf.getInt(
            DFS_EC_CONVERT_MAX_CONCURRNET_JOBS,
            DFS_EC_CONVERT_MAX_CONCURRNET_JOBS_DEFAULT);
    listStatusThreads = conf.getInt(
        DFS_EC_CONVERT_NUM_LISTSTATUS_THREADS,
        DFS_EC_CONVERT_NUM_LISTSTATUS_THREADS_DEFAULT);
    ecMaxTasks = conf.getInt(
        DFS_EC_CONVERT_MAX_MAP_TASKS, DFS_EC_CONVERT_MAX_MAP_TASKS_DEFAULT);
    blocksPerChunk = conf.getInt(DFS_EC_CONVERT_BLOCKS_PER_CHUNK,
        DFS_EC_CONVERT_BLOCKS_PER_CHUNK_DEFAULT);

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
      if (srcEcPaths[i].isEmpty()) {
        continue;
      }

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
