/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.RollbackHelperV1;
import org.apache.hudi.table.marker.AppendMarkerHandler;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An implementation using markers to identify missing files for table version 6.
 * In spark MOR table, task retries may generate log files which are not included in write status.
 * We need to add these to CommitMetadata so that it will be synced to MDT.
 */
public class MarkerBasedCommitMetadataResolver implements CommitMetadataResolver {
  @Override
  public HoodieCommitMetadata reconcileMetadataForMissingFiles(HoodieWriteConfig config,
                                                               HoodieEngineContext context,
                                                               HoodieTable table,
                                                               String instantTime,
                                                               HoodieCommitMetadata commitMetadata) throws HoodieIOException {
    try {
      AppendMarkerHandler markers = WriteMarkersFactory.getAppendMarkerHandler(config.getMarkersType(), table, instantTime);
      // if there is log files in this delta commit, we search any invalid log files generated by failed spark task
      boolean hasLogFileInDeltaCommit = commitMetadata.getPartitionToWriteStats()
          .values().stream().flatMap(List::stream)
          .anyMatch(writeStat -> FSUtils.isLogFile(new StoragePath(config.getBasePath(), writeStat.getPath()).getName()));
      if (hasLogFileInDeltaCommit) { // skip for COW table
        // get all log files generated by makers
        Set<String> allLogFilesMarkerPath = new HashSet<>(markers.getAppendedLogPaths(context, config.getFinalizeWriteParallelism()));
        Set<String> logFilesMarkerPath = new HashSet<>();
        allLogFilesMarkerPath.stream().filter(logFilePath -> !logFilePath.endsWith("cdc")).forEach(logFilesMarkerPath::add);

        // remove valid log files
        // TODO: refactor based on HoodieData
        for (Map.Entry<String, List<HoodieWriteStat>> partitionAndWriteStats : commitMetadata.getPartitionToWriteStats().entrySet()) {
          for (HoodieWriteStat hoodieWriteStat : partitionAndWriteStats.getValue()) {
            logFilesMarkerPath.remove(hoodieWriteStat.getPath());
          }
        }

        // remaining are log files generated by retried spark task, let's generate write stat for them
        if (!logFilesMarkerPath.isEmpty()) {
          context.setJobStatus(this.getClass().getName(),
              "Preparing data for missing files to assist with generating write stats");
          // populate partition -> map (fileId -> HoodieWriteStat) // we just need one write stat per fileID to fetch some info about
          // the file slice of interest to populate WriteStat.
          HoodiePairData<String, Map<String, HoodieWriteStat>> partitionToWriteStatHoodieData = getPartitionToFileIdToFilesMap(commitMetadata, context);

          String basePathStr = config.getBasePath();
          // populate partition -> map (fileId -> List <missing log file names>)
          HoodiePairData<String, Map<String, List<String>>> partitionToMissingLogFilesHoodieData =
              getPartitionToFileIdToMissingLogFileMap(basePathStr, logFilesMarkerPath, context, config.getFileListingParallelism());

          context.setJobStatus(this.getClass().getName(),
              "Generating writeStat for missing log files");

          // lets join both to generate write stats for missing log files
          List<Pair<String, List<HoodieWriteStat>>> additionalLogFileWriteStat = getWriteStatsForMissingLogFiles(partitionToWriteStatHoodieData,
              partitionToMissingLogFilesHoodieData, table.getStorageConf(), basePathStr);

          for (Pair<String, List<HoodieWriteStat>> partitionDeltaStats : additionalLogFileWriteStat) {
            String partitionPath = partitionDeltaStats.getKey();
            partitionDeltaStats.getValue().forEach(ws -> commitMetadata.addWriteStat(partitionPath, ws));
          }
        }
      }
      return commitMetadata;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to fix commit metadata for spurious log files "
          + config.getBasePath() + " at time " + instantTime, e);
    }
  }

  /**
   * Get partition path to fileId to write stat map.
   */
  private static HoodiePairData<String, Map<String, HoodieWriteStat>> getPartitionToFileIdToFilesMap(HoodieCommitMetadata commitMetadata, HoodieEngineContext context) {
    List<Map.Entry<String, List<HoodieWriteStat>>> partitionToWriteStats = new ArrayList<>(commitMetadata.getPartitionToWriteStats().entrySet());

    return context.parallelize(partitionToWriteStats)
        .mapToPair((SerializablePairFunction<Map.Entry<String, List<HoodieWriteStat>>, String, Map<String, HoodieWriteStat>>) t -> {
          Map<String, HoodieWriteStat> fileIdToWriteStat = new HashMap<>();
          t.getValue().forEach(writeStat -> {
            if (!fileIdToWriteStat.containsKey(writeStat.getFileId())) {
              fileIdToWriteStat.put(writeStat.getFileId(), writeStat);
            }
          });
          return Pair.of(t.getKey(), fileIdToWriteStat);
        });
  }

  /**
   * Get partition path to fileId to missing log file map.
   *
   * @param basePathStr        base path
   * @param logFilesMarkerPath set of log file marker paths
   * @param context            HoodieEngineContext
   * @param parallelism        parallelism
   * @return HoodiePairData of partition path to fileId to missing log file map.
   */
  private static HoodiePairData<String, Map<String, List<String>>> getPartitionToFileIdToMissingLogFileMap(String basePathStr, Set<String> logFilesMarkerPath, HoodieEngineContext context,
                                                                                                           int parallelism) {
    List<String> logFilePaths = new ArrayList<>(logFilesMarkerPath);
    HoodiePairData<String, List<String>> partitionPathLogFilePair = context.parallelize(logFilePaths).mapToPair(logFilePath -> {
      StoragePath logFileFullPath = new StoragePath(basePathStr, logFilePath);
      String partitionPath = FSUtils.getRelativePartitionPath(new StoragePath(basePathStr), logFileFullPath.getParent());
      return Pair.of(partitionPath, Collections.singletonList(logFileFullPath.getName()));
    });
    HoodiePairData<String, Map<String, List<String>>> partitionPathToFileIdAndLogFileList = partitionPathLogFilePair
        // reduce by partition paths
        .reduceByKey((SerializableBiFunction<List<String>, List<String>, List<String>>) (strings, strings2) -> {
          List<String> logFilePaths1 = new ArrayList<>(strings);
          logFilePaths1.addAll(strings2);
          return logFilePaths1;
        }, parallelism).mapToPair((SerializablePairFunction<Pair<String, List<String>>, String, Map<String, List<String>>>) t -> {
          // for each hudi partition, collect list of missing log files, fetch file size using file system calls, and populate fileId -> List<FileStatus> map

          String partitionPath = t.getKey();
          StoragePath fullPartitionPath = StringUtils.isNullOrEmpty(partitionPath) ? new StoragePath(basePathStr) : new StoragePath(basePathStr, partitionPath);
          // fetch file sizes from FileSystem
          List<String> missingLogFiles = t.getValue();
          Map<String, List<String>> fileIdtologFiles = new HashMap<>();
          missingLogFiles.forEach(logFile -> {
            String fileId = FSUtils.getFileIdFromLogPath(new StoragePath(fullPartitionPath, logFile));
            if (!fileIdtologFiles.containsKey(fileId)) {
              fileIdtologFiles.put(fileId, new ArrayList<>());
            }
            fileIdtologFiles.get(fileId).add(logFile);
          });
          return Pair.of(partitionPath, fileIdtologFiles);
        });
    return partitionPathToFileIdAndLogFileList;
  }

  /**
   * Generate write stats for missing log files. Performs an inner join on partition between existing
   * partitionToWriteStatHoodieData and partitionToMissingLogFilesHoodieData.
   * For missing log files, it does one file system call to fetch file size (FSUtils#getPathInfoUnderPartition).
   */
  private static List<Pair<String, List<HoodieWriteStat>>> getWriteStatsForMissingLogFiles(HoodiePairData<String, Map<String, HoodieWriteStat>> partitionToWriteStatHoodieData,
                                                                                           HoodiePairData<String, Map<String, List<String>>> partitionToMissingLogFilesHoodieData,
                                                                                           StorageConfiguration<?> storageConfiguration,
                                                                                           String basePathStr) {
    // lets join both to generate write stats for missing log files
    return partitionToWriteStatHoodieData
        .join(partitionToMissingLogFilesHoodieData)
        .map((SerializableFunction<Pair<String, Pair<Map<String, HoodieWriteStat>, Map<String, List<String>>>>, Pair<String, List<HoodieWriteStat>>>) v1 -> {
          final StoragePath basePathLocal = new StoragePath(basePathStr);
          String partitionPath = v1.getKey();
          Map<String, HoodieWriteStat> fileIdToOriginalWriteStat = v1.getValue().getKey();
          Map<String, List<String>> missingFileIdToLogFileNames = v1.getValue().getValue();
          List<String> missingLogFileNames = missingFileIdToLogFileNames.values().stream()
              .flatMap(List::stream)
              .collect(Collectors.toList());

          // fetch file sizes from FileSystem
          StoragePath fullPartitionPath = StringUtils.isNullOrEmpty(partitionPath) ? new StoragePath(basePathStr) : new StoragePath(basePathStr, partitionPath);
          HoodieStorage storage = HoodieStorageUtils.getStorage(fullPartitionPath, storageConfiguration);
          List<Option<StoragePathInfo>> storageInfosOpt = RollbackHelperV1.getPathInfoUnderPartition(storage, fullPartitionPath, new HashSet<>(missingLogFileNames), true);
          List<StoragePathInfo> storagePathInfos = storageInfosOpt.stream()
              .filter(fileStatusOpt -> fileStatusOpt.isPresent())
              .map(fileStatusOption -> fileStatusOption.get())
              .collect(Collectors.toList());

          // populate fileId -> List<FileStatus>
          Map<String, List<StoragePathInfo>> missingFileIdToLogFilesList = new HashMap<>();
          storagePathInfos.forEach(storagePathInfo -> {
            String fileId = FSUtils.getFileIdFromLogPath(storagePathInfo.getPath());
            missingFileIdToLogFilesList.putIfAbsent(fileId, new ArrayList<>());
            missingFileIdToLogFilesList.get(fileId).add(storagePathInfo);
          });

          List<HoodieWriteStat> missingWriteStats = new ArrayList();
          missingFileIdToLogFilesList.forEach((k, logPathInfos) -> {
            String fileId = k;
            HoodieDeltaWriteStat originalWriteStat =
                (HoodieDeltaWriteStat) fileIdToOriginalWriteStat.get(fileId); // are there chances that there won't be any write stat in original list?
            logPathInfos.forEach(logPathInfo -> {
              // for every missing file, add a new HoodieDeltaWriteStat
              HoodieDeltaWriteStat writeStat = getHoodieDeltaWriteStatFromPreviousStat(logPathInfo, basePathLocal,
                  partitionPath, fileId, originalWriteStat);
              missingWriteStats.add(writeStat);
            });
          });
          return Pair.of(partitionPath, missingWriteStats);
        }).collectAsList();
  }

  private static HoodieDeltaWriteStat getHoodieDeltaWriteStatFromPreviousStat(StoragePathInfo storagePathInfo,
                                                                              StoragePath basePathLocal,
                                                                              String partitionPath,
                                                                              String fileId,
                                                                              HoodieDeltaWriteStat originalWriteStat) {
    HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
    HoodieLogFile logFile = new HoodieLogFile(storagePathInfo);
    writeStat.setPath(basePathLocal, logFile.getPath());
    writeStat.setPartitionPath(partitionPath);
    writeStat.setFileId(fileId);
    writeStat.setTotalWriteBytes(logFile.getFileSize());
    writeStat.setFileSizeInBytes(logFile.getFileSize());
    writeStat.setLogVersion(logFile.getLogVersion());
    List<String> logFiles = new ArrayList<>(originalWriteStat.getLogFiles());
    logFiles.add(logFile.getFileName());
    writeStat.setLogFiles(logFiles);
    writeStat.setBaseFile(originalWriteStat.getBaseFile());
    writeStat.setPrevCommit(logFile.getDeltaCommitTime());
    return writeStat;
  }
}
