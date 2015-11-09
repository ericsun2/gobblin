/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.logs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractScheduledService;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.DatasetFilterUtils;
import gobblin.util.ExecutorsUtils;
import gobblin.util.FileListUtils;


/**
 * A utility class that periodically reads log files in a source log file directory for changes
 * since the last reads and appends the changes to destination log files with the same names as
 * the source log files in a destination log directory. The source and destination log files
 * can be on different {@link FileSystem}s.
 *
 * <p>
 *   This class extends the {@link AbstractScheduledService} so it can be used with a
 *   {@link com.google.common.util.concurrent.ServiceManager} that manages the lifecycle of
 *   a {@link LogCopier}.
 * </p>
 *
 * <p>
 *   This class is intended to be used in the following pattern:
 *
 *   <pre>
 *     {@code
 *       LogCopier.Builder logCopierBuilder = LogCopier.newBuilder();
 *       LogCopier logCopier = logCopierBuilder
 *           .useSrcFileSystem(FileSystem.getLocal(new Configuration()))
 *           .useDestFileSystem(FileSystem.get(URI.create(destFsUri), new Configuration()))
 *           .readFrom(new Path(srcLogDir))
 *           .writeTo(new Path(destLogDir))
 *           .useInitialDelay(60)
 *           .useCopyInterval(60)
 *           .useTimeUnit(TimeUnit.SECONDS)
 *           .build();
 *
 *       ServiceManager serviceManager = new ServiceManager(Lists.newArrayList(logCopier));
 *       serviceManager.startAsync();
 *
 *       // ...
 *       serviceManager.stopAsync().awaitStopped(60, TimeUnit.SECONDS);
 *     }
 *   </pre>
 *
 *   Checkout the Javadoc of {@link LogCopier.Builder} to see the available options for customization.
 * </p>
 *
 * @author ynli
 */
public class LogCopier extends AbstractScheduledService {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogCopier.class);

  private static final long DEFAULT_SOURCE_LOG_FILE_MONITOR_INTERVAL = 120;
  private static final long DEFAULT_LOG_COPY_INTERVAL_SECONDS = 60;
  private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
  private static final int DEFAULT_LINES_WRITTEN_BEFORE_FLUSH = 100;

  private final FileSystem srcFs;
  private final FileSystem destFs;
  private final Path srcLogDir;
  private final Path destLogDir;

  private final long sourceLogFileMonitorInterval;
  private final long copyInterval;
  private final TimeUnit timeUnit;

  private final Set<String> logFileExtensions;

  private final Optional<List<Pattern>> includingRegexPatterns;
  private final Optional<List<Pattern>> excludingRegexPatterns;

  private final Optional<String> logFileNamePrefix;

  private final int linesWrittenBeforeFlush;

  private final ScheduledExecutorService logCopyExecutor;

  // A map from source log file paths to an entry that stores the ScheduledFuture of the copy
  // task for the log file and the current position to which the copier has copied to. This
  // is accessed by a single thread so no synchronization is needed.
  private final Map<Path, ScheduledFuture<?>> logCopyTaskMap = Maps.newHashMap();

  private LogCopier(Builder builder) {
    this.srcFs = builder.srcFs;
    this.destFs = builder.destFs;

    this.srcLogDir = this.srcFs.makeQualified(builder.srcLogDir);
    this.destLogDir = this.destFs.makeQualified(builder.destLogDir);

    this.sourceLogFileMonitorInterval = builder.sourceLogFileMonitorInterval;
    this.copyInterval = builder.copyInterval;
    this.timeUnit = builder.timeUnit;

    this.logFileExtensions = builder.logFileExtensions;

    this.includingRegexPatterns = Optional.fromNullable(builder.includingRegexPatterns);
    this.excludingRegexPatterns = Optional.fromNullable(builder.excludingRegexPatterns);

    this.logFileNamePrefix = Optional.fromNullable(builder.logFileNamePrefix);

    this.linesWrittenBeforeFlush = builder.linesWrittenBeforeFlush;

    this.logCopyExecutor = Executors.newScheduledThreadPool(0, ExecutorsUtils
        .newThreadFactory(Optional.of(LOGGER), Optional.of("LogCopyExecutor")));
  }

  @Override
  protected void shutDown() throws Exception {
    ExecutorsUtils.shutdownExecutorService(this.logCopyExecutor, Optional.of(LOGGER));
  }

  @Override
  protected void runOneIteration() throws IOException {
    checkSrcLogFiles();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, this.sourceLogFileMonitorInterval, this.timeUnit);
  }

  /**
   * Perform a check on new source log files and submit copy tasks for new log files.
   */
  private void checkSrcLogFiles() throws IOException {
    List<FileStatus> srcLogFiles = FileListUtils.listFilesRecursively(this.srcFs, this.srcLogDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return logFileExtensions.contains(Files.getFileExtension(path.getName()));
      }
    });

    if (srcLogFiles.isEmpty()) {
      LOGGER.warn("No log file found under directory " + this.srcLogDir);
      return;
    }

    Set<Path> newLogFiles = Sets.newHashSet();
    for (FileStatus srcLogFile : srcLogFiles) {
      newLogFiles.add(srcLogFile.getPath());
    }

    Set<Path> deletedLogFiles = Sets.newHashSet(this.logCopyTaskMap.keySet());
    // Compute the set of deleted log files since the last check
    deletedLogFiles.removeAll(newLogFiles);
    // Compute the set of new log files since the last check
    newLogFiles.removeAll(this.logCopyTaskMap.keySet());

    // Schedule a copy task for each new log file
    for (final Path srcLogFile : newLogFiles) {
      String destLogFileName = this.logFileNamePrefix.isPresent() ?
          this.logFileNamePrefix.get() + "." + srcLogFile.getName() : srcLogFile.getName();
      final Path destLogFile = new Path(this.destLogDir, destLogFileName);

      ScheduledFuture<?> copyFuture = this.logCopyExecutor.scheduleAtFixedRate(
          new LogCopyTask(srcLogFile, destLogFile), 0, this.copyInterval, this.timeUnit);
      this.logCopyTaskMap.put(srcLogFile, copyFuture);
    }

    // Cancel the copy task for each deleted log file
    for (Path deletedLogFile : deletedLogFiles) {
      this.logCopyTaskMap.remove(deletedLogFile).cancel(true);
    }
  }

  /**
   * Get a new {@link LogCopier.Builder} instance for building a {@link LogCopier}.
   *
   * @return a new {@link LogCopier.Builder} instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * A builder class for {@link LogCopier}.
   */
  public static class Builder {

    private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private FileSystem srcFs;
    private Path srcLogDir;
    private FileSystem destFs;
    private Path destLogDir;

    private long sourceLogFileMonitorInterval = DEFAULT_SOURCE_LOG_FILE_MONITOR_INTERVAL;
    private long copyInterval = DEFAULT_LOG_COPY_INTERVAL_SECONDS;
    private TimeUnit timeUnit = DEFAULT_TIME_UNIT;

    private Set<String> logFileExtensions;

    private List<Pattern> includingRegexPatterns;
    private List<Pattern> excludingRegexPatterns;

    private String logFileNamePrefix;

    private int linesWrittenBeforeFlush = DEFAULT_LINES_WRITTEN_BEFORE_FLUSH;

    /**
     * Set the interval between two checks for the source log file monitor.
     *
     * @param sourceLogFileMonitorInterval the interval between two checks for the source log file monitor
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useSourceLogFileMonitorInterval(long sourceLogFileMonitorInterval) {
      Preconditions.checkArgument(sourceLogFileMonitorInterval > 0,
          "Source log file monitor interval must be positive");
      this.sourceLogFileMonitorInterval = sourceLogFileMonitorInterval;
      return this;
    }

    /**
     * Set the copy interval between two iterations of copies.
     *
     * @param copyInterval the copy interval between two iterations of copies
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useCopyInterval(long copyInterval) {
      Preconditions.checkArgument(copyInterval > 0, "Copy interval must be positive");
      this.copyInterval = copyInterval;
      return this;
    }

    /**
     * Set the {@link TimeUnit} used for the source log file monitor interval, initial delay, copy interval.
     *
     * @param timeUnit the {@link TimeUnit} used for the initial delay and copy interval
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useTimeUnit(TimeUnit timeUnit) {
      Preconditions.checkNotNull(timeUnit);
      this.timeUnit = timeUnit;
      return this;
    }

    /**
     * Set the set of acceptable log file extensions.
     *
     * @param logFileExtensions the set of acceptable log file extensions
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder acceptsLogFileExtensions(Set<String> logFileExtensions) {
      Preconditions.checkNotNull(logFileExtensions);
      this.logFileExtensions = ImmutableSet.copyOf(logFileExtensions);
      return this;
    }

    /**
     * Set the regex patterns used to filter logs that should be copied.
     *
     * @param regexList a comma-separated list of regex patterns
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useIncludingRegexPatterns(String regexList) {
      Preconditions.checkNotNull(regexList);
      this.includingRegexPatterns = DatasetFilterUtils.getPatternsFromStrings(COMMA_SPLITTER.splitToList(regexList));
      return this;
    }

    /**
     * Set the regex patterns used to filter logs that should not be copied.
     *
     * @param regexList a comma-separated list of regex patterns
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useExcludingRegexPatterns(String regexList) {
      Preconditions.checkNotNull(regexList);
      this.excludingRegexPatterns = DatasetFilterUtils.getPatternsFromStrings(COMMA_SPLITTER.splitToList(regexList));
      return this;
    }

    /**
     * Set the source {@link FileSystem} for reading the source log file.
     *
     * @param srcFs the source {@link FileSystem} for reading the source log file
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useSrcFileSystem(FileSystem srcFs) {
      Preconditions.checkNotNull(srcFs);
      this.srcFs = srcFs;
      return this;
    }

    /**
     * Set the destination {@link FileSystem} for writing the destination log file.
     *
     * @param destFs the destination {@link FileSystem} for writing the destination log file
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useDestFileSystem(FileSystem destFs) {
      Preconditions.checkNotNull(destFs);
      this.destFs = destFs;
      return this;
    }

    /**
     * Set the path of the source log file directory to read from.
     *
     * @param srcLogDir the path of the source log file directory to read from
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder readFrom(Path srcLogDir) {
      Preconditions.checkNotNull(srcLogDir);
      this.srcLogDir = srcLogDir;
      return this;
    }

    /**
     * Set the path of the destination log file directory to write to.
     *
     * @param destLogDir the path of the destination log file directory to write to
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder writeTo(Path destLogDir) {
      Preconditions.checkNotNull(destLogDir);
      this.destLogDir = destLogDir;
      return this;
    }

    /**
     * Set the log file name prefix at the destination.
     *
     * @param logFileNamePrefix the log file name prefix at the destination
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useLogFileNamePrefix(String logFileNamePrefix) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(logFileNamePrefix),
          "Invalid log file name prefix: " + logFileNamePrefix);
      this.logFileNamePrefix = logFileNamePrefix;
      return this;
    }

    /**
     * Set the number of lines written before they are flushed to disk.
     *
     * @param linesWrittenBeforeFlush the number of lines written before they are flushed to disk
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useLinesWrittenBeforeFlush(int linesWrittenBeforeFlush) {
      Preconditions.checkArgument(linesWrittenBeforeFlush > 0,
          "The value specifying the lines to write before flush must be positive");
      this.linesWrittenBeforeFlush = linesWrittenBeforeFlush;
      return this;
    }

    /**
     * Build a new {@link LogCopier} instance.
     *
     * @return a new {@link LogCopier} instance
     */
    public LogCopier build() {
      return new LogCopier(this);
    }
  }

  /**
   * A log copy task that manages the current source log file position itself.
   */
  private class LogCopyTask implements Runnable {

    private final Path srcLogFile;
    private final Path destLogFile;

    // The task maintains the current source log file position itself
    private long currentPos = 0;

    LogCopyTask(Path srcLogFile, Path destLogFile) {
      this.srcLogFile = srcLogFile;
      this.destLogFile = destLogFile;
    }

    @Override
    public void run() {
      try {
        LOGGER.debug(String.format("Copying changes from %s to %s", this.srcLogFile, this.destLogFile));
        copyChangesOfLogFile(srcFs.makeQualified(this.srcLogFile), destFs.makeQualified(this.destLogFile));
      } catch (IOException ioe) {
        LOGGER.error(String.format("Failed while copying logs from %s to %s", this.srcLogFile, this.destLogFile), ioe);
      }
    }

    /**
     * Copy changes for a single log file.
     */
    private void copyChangesOfLogFile(Path srcFile, Path destFile) throws IOException {
      if (!srcFs.exists(srcFile)) {
        LOGGER.warn("Source log file not found: " + srcFile);
        return;
      }

      Closer closer = Closer.create();
      // We need to use fsDataInputStream in the finally clause so it has to be defined outside try-catch-finally
      FSDataInputStream fsDataInputStream = null;

      try {
        fsDataInputStream = closer.register(srcFs.open(srcFile));
        // Seek to the the most recent position if it is available
        LOGGER.debug(String.format("Reading log file %s from position %d", srcFile, this.currentPos));
        fsDataInputStream.seek(this.currentPos);
        BufferedReader srcLogFileReader = closer.register(
            new BufferedReader(new InputStreamReader(fsDataInputStream, ConfigurationKeys.DEFAULT_CHARSET_ENCODING)));

        FSDataOutputStream outputStream = destFs.exists(destFile) ? destFs.append(destFile) : destFs.create(destFile);
        BufferedWriter destLogFileWriter = closer.register(
            new BufferedWriter(new OutputStreamWriter(outputStream, ConfigurationKeys.DEFAULT_CHARSET_ENCODING)));

        String line;
        int linesProcessed = 0;
        while (!Thread.currentThread().isInterrupted() && (line = srcLogFileReader.readLine()) != null) {
          if (!shouldCopyLine(line)) {
            continue;
          }

          destLogFileWriter.write(line);
          destLogFileWriter.newLine();
          linesProcessed++;
          if (linesProcessed % linesWrittenBeforeFlush == 0) {
            destLogFileWriter.flush();
          }
        }
      } catch (Throwable t) {
        throw closer.rethrow(t);
      } finally {
        try {
          closer.close();
        } finally {
          if (fsDataInputStream != null) {
            this.currentPos = fsDataInputStream.getPos();
          }
        }
      }
    }

    /**
     * Check if a log line should be copied.
     *
     * <p>
     *   A line should be copied if and only if all of the following conditions satisfy:
     *
     *   <ul>
     *     <li>
     *       It doesn't match any of the excluding regex patterns. If there's no excluding regex patterns,
     *       this condition is considered satisfied.
     *     </li>
     *     <li>
     *       It matches at least one of the including regex patterns. If there's no including regex patterns,
     *       this condition is considered satisfied.
     *     </li>
     *   </ul>
     * </p>
     */
    private boolean shouldCopyLine(String line) {
      boolean including = !includingRegexPatterns.isPresent() ||
          DatasetFilterUtils.stringInPatterns(line, includingRegexPatterns.get());
      boolean excluding = excludingRegexPatterns.isPresent() &&
          DatasetFilterUtils.stringInPatterns(line, excludingRegexPatterns.get());

      return !excluding && including;
    }
  }
}