package org.apache.hadoop.mapred.history;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.*;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

/**
 * Direct read job history file as HSClientProtocol
 */
public class DirectHistoryProxy implements HSClientProtocol, Tool, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(DirectHistoryProxy.class);
  private Configuration conf;
  private String userName;
  private String jobDate;
  private FileSystem fs;
  private LoadingCache<JobId, Job> jobCache;
  private int searchDays;
  private boolean loadTasks;
  private RecordFactory recordFactory;


  public DirectHistoryProxy() {
  }

  public void init() throws IOException {
    this.userName = conf.get("tq.mapreduce.history.job.user");
    this.jobDate = conf.get("tq.mapreduce.history.job.date");
    this.fs = FileSystem.get(conf);
    final int cacheNum = conf.getInt("tq.mapreduce.history.job.cache.num", 1024);
    this.searchDays = conf.getInt("tq.mapreduce.history.job.search.days", 7);
    this.loadTasks = conf.getBoolean("tq.mapreduce.history.job.load.tasks", false);
    this.jobCache = CacheBuilder.newBuilder().maximumSize(cacheNum).build(new CacheLoader<JobId, Job>() {
      @Override
      public Job load(JobId key) throws Exception {
        return verifyAndGetJob(key);
      }
    });
    this.recordFactory = RecordFactoryProvider.getRecordFactory(conf);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Init DirectHistoryProxy ");
    }
  }

  private HistorySearchResult getJobHistoryFile(JobId jobId) throws IOException {
    int loop = 1;
    String jobUser = userName;
    if (jobUser == null) {
      jobUser = UserGroupInformation.getCurrentUser().getShortUserName();
    }
    // try search
    if (JobHistoryUtils.isIntermediateDoneDirDateFormattedEnable(conf) && jobDate == null) {
      loop = searchDays;
    }
    Date date = new Date();
    String userDoneDir;
    for (int i = 0; i < loop; i++) {
      if (JobHistoryUtils.isIntermediateDoneDirDateFormattedEnable(conf)) {
        if (jobDate == null) {
          userDoneDir = JobHistoryUtils.getHistoryIntermediateDoneDirForUser(conf, date, jobUser);
          // try last day
          date.setTime(date.getTime() - 24 * 60 * 60 * 1000L);
        } else {
          userDoneDir = JobHistoryUtils.getHistoryIntermediateDoneDirForUser(conf, jobDate, jobUser);
        }
      } else {
        userDoneDir = JobHistoryUtils.getHistoryIntermediateDoneDirForUser(conf, jobUser);
      }

      Path userDonePath = fs.makeQualified(new Path(userDoneDir));
      //1. try index file
      if (conf.getBoolean("tq.mapreduce.write.history.index", true)) {
        Path qualifiedIndexFile = new Path(userDonePath,
            JobHistoryUtils.getIntermediateJobHistoryIndexFileName(jobId));
        try {
          if (fs.exists(qualifiedIndexFile)) {
            FSDataInputStream inputStream = null;
            try {
              inputStream = fs.open(qualifiedIndexFile);
              return HistorySearchResult.of(userDonePath, inputStream.readUTF());
            } finally {
              if (inputStream != null) {
                inputStream.close();
              }
            }
          }
        } catch (Exception e) {
          LOG.warn("Error reading job index file for " + qualifiedIndexFile + " : " + e.getMessage());
        }
      }
      //2. try global list
      FileStatus[] fileStatuses = fs.globStatus(new Path(userDonePath,
          jobId + "*" + JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION));
      if (fileStatuses != null && fileStatuses.length > 0) {
        FileStatus fileStatus = fileStatuses[0];
        return HistorySearchResult.of(fileStatus.getPath().getParent(), fileStatus.getPath().getName());
      }
    }

    throw new IOException("Not found history file for:" + jobId);
  }

  private Job verifyAndGetJob(final JobId jobId)
      throws IOException {
    Job job = null;
    try {
      job = UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Job>() {
        HistorySearchResult historyRet = getJobHistoryFile(jobId);
        Path historyConfFile = new Path(historyRet.getUserDonePath(),
            JobHistoryUtils.getIntermediateConfFileName(jobId));
        Path historyFile = new Path(historyRet.getUserDonePath(), historyRet.getHistoryFileName());

        @Override
        public Job run() throws Exception {
          return new CompletedJob(conf, jobId, historyFile, loadTasks, userName, historyConfFile,
              new JobACLsManager(conf));
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    if (job == null) {
      throw new IOException("Unknown Job :" + jobId);
    }

    JobACL operation = JobACL.VIEW_JOB;
    job.checkAccess(UserGroupInformation.getCurrentUser(), operation);
    return job;
  }

  public Job getJob(JobId jobID, boolean exceptionThrow) throws IOException {
    try {
      return jobCache.get(jobID);
    } catch (Exception e) {
      if (exceptionThrow) {
        throw new IOException(e);
      }
    }
    return null;
  }

  @Override
  public GetCountersResponse getCounters(GetCountersRequest request)
      throws IOException {
    JobId jobId = request.getJobId();
    Job job = getJob(jobId, true);
    GetCountersResponse response = recordFactory.newRecordInstance(GetCountersResponse.class);
    response.setCounters(TypeConverter.toYarn(job.getAllCounters()));
    return response;
  }

  @Override
  public InetSocketAddress getConnectAddress() {
    return null;
  }

  @Override
  public GetJobReportResponse getJobReport(GetJobReportRequest request)
      throws IOException {
    JobId jobId = request.getJobId();
    Job job = getJob(jobId, false);
    GetJobReportResponse response = recordFactory.newRecordInstance(GetJobReportResponse.class);
    if (job != null) {
      response.setJobReport(job.getReport());
    } else {
      response.setJobReport(null);
    }
    return response;
  }

  @Override
  public GetTaskAttemptReportResponse getTaskAttemptReport(
      GetTaskAttemptReportRequest request) throws IOException {
    TaskAttemptId taskAttemptId = request.getTaskAttemptId();
    Job job = getJob(taskAttemptId.getTaskId().getJobId(), true);
    GetTaskAttemptReportResponse response = recordFactory.newRecordInstance(GetTaskAttemptReportResponse.class);
    response.setTaskAttemptReport(job.getTask(taskAttemptId.getTaskId()).getAttempt(taskAttemptId).getReport());
    return response;
  }

  @Override
  public GetTaskReportResponse getTaskReport(GetTaskReportRequest request)
      throws IOException {
    TaskId taskId = request.getTaskId();
    Job job = getJob(taskId.getJobId(), true);
    GetTaskReportResponse response = recordFactory.newRecordInstance(GetTaskReportResponse.class);
    response.setTaskReport(job.getTask(taskId).getReport());
    return response;
  }

  @Override
  public GetTaskAttemptCompletionEventsResponse
  getTaskAttemptCompletionEvents(
      GetTaskAttemptCompletionEventsRequest request) throws IOException {
    JobId jobId = request.getJobId();
    int fromEventId = request.getFromEventId();
    int maxEvents = request.getMaxEvents();
    Job job = getJob(jobId, true);
    GetTaskAttemptCompletionEventsResponse response = recordFactory.newRecordInstance(GetTaskAttemptCompletionEventsResponse.class);
    response.addAllCompletionEvents(Arrays.asList(job.getTaskAttemptCompletionEvents(fromEventId, maxEvents)));
    return response;
  }

  @Override
  public KillJobResponse killJob(KillJobRequest request) throws IOException {
    throw new IOException("Invalid operation on completed job");
  }

  @Override
  public KillTaskResponse killTask(KillTaskRequest request)
      throws IOException {
    throw new IOException("Invalid operation on completed job");
  }

  @Override
  public KillTaskAttemptResponse killTaskAttempt(
      KillTaskAttemptRequest request) throws IOException {
    throw new IOException("Invalid operation on completed job");
  }

  @Override
  public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request)
      throws IOException {
    TaskAttemptId taskAttemptId = request.getTaskAttemptId();
    Job job = getJob(taskAttemptId.getTaskId().getJobId(), true);
    GetDiagnosticsResponse response = recordFactory.newRecordInstance(GetDiagnosticsResponse.class);
    response.addAllDiagnostics(job.getTask(taskAttemptId.getTaskId()).getAttempt(taskAttemptId).getDiagnostics());
    return response;
  }

  @Override
  public FailTaskAttemptResponse failTaskAttempt(
      FailTaskAttemptRequest request) throws IOException {
    throw new IOException("Invalid operation on completed job");
  }

  @Override
  public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request)
      throws IOException {
    JobId jobId = request.getJobId();
    TaskType taskType = request.getTaskType();

    GetTaskReportsResponse response = recordFactory.newRecordInstance(GetTaskReportsResponse.class);
    Job job = getJob(jobId, true);
    Collection<Task> tasks = job.getTasks(taskType).values();
    for (Task task : tasks) {
      response.addTaskReport(task.getReport());
    }
    return response;
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws IOException {
    throw new IOException("Invalid operation on completed job");
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws IOException {
    throw new IOException("Invalid operation on completed job");
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws IOException {
    throw new IOException("Invalid operation on completed job");
  }

  @Override
  public void close() throws IOException {
    jobCache.invalidateAll();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }


  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args == null || args.length == 0) {
      System.err.println("Missing job id");
      return -1;
    }
    init();
    Job job = getJob(TypeConverter.toYarn(JobID.forName(args[0])), true);
    ((CompletedJob) job).printJobInfo();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DirectHistoryProxy(), args);
  }
}

class HistorySearchResult {
  private Path userDonePath;
  private String historyFileName;

  public HistorySearchResult(Path userDonePath, String historyFileName) {
    this.userDonePath = userDonePath;
    this.historyFileName = historyFileName;
  }

  public Path getUserDonePath() {
    return userDonePath;
  }

  public String getHistoryFileName() {
    return historyFileName;
  }

  public static HistorySearchResult of(Path userDonePath, String historyFileName) {
    return new HistorySearchResult(userDonePath, historyFileName);
  }
}
