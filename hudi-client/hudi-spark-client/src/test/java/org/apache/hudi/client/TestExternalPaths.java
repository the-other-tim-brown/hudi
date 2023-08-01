/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.action.clean.CleanPlanner;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.reflect.ClassTag;

import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;

public class TestExternalPaths extends HoodieClientTestBase {

  private HoodieWriteConfig writeConfig;

  @ParameterizedTest
  @MethodSource("getArgs")
  public void testFlow(FileIdAndNameGenerator fileIdAndNameGenerator, List<String> partitions) throws Exception {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    writeConfig = HoodieWriteConfig.newBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(INMEMORY).build())
        .withPath(metaClient.getBasePathV2().toString())
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(2)
            .enable(true).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(1, 2).build())
        .withTableServicesEnabled(true)
        .build();

    writeClient = new SparkRDDWriteClient(context, writeConfig, false, Option.empty());
    String instantTime1 = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, metaClient);
    String partitionPath1 = partitions.get(0);
    Pair<String, String> fileIdAndName1 = fileIdAndNameGenerator.generate(1, instantTime1);
    String fileId1 = fileIdAndName1.getLeft();
    String fileName1 = fileIdAndName1.getRight();
    String filePath1 = getPath(partitionPath1, fileName1);
    WriteStatus writeStatus1 = createWriteStatus(partitionPath1, filePath1, fileId1);
    JavaRDD<WriteStatus> rdd1 = createRdd(Arrays.asList(writeStatus1));
    metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime1), Option.empty());
    writeClient.commit(instantTime1, rdd1, Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION,  Collections.emptyMap());

    assertFileGroupCorrectness(instantTime1, partitionPath1, filePath1, fileId1, 1);

    // add a new file and remove the old one
    String instantTime2 = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, metaClient);
    Pair<String, String> fileIdAndName2 = fileIdAndNameGenerator.generate(2, instantTime2);
    String fileId2 = fileIdAndName2.getLeft();
    String fileName2 = fileIdAndName2.getRight();
    String filePath2 = getPath(partitionPath1, fileName2);
    WriteStatus newWriteStatus = createWriteStatus(partitionPath1, filePath2, fileId2);
    JavaRDD<WriteStatus> rdd2 = createRdd(Arrays.asList(newWriteStatus));
    Map<String, List<String>> partitionToReplacedFileIds = new HashMap<>();
    partitionToReplacedFileIds.put(partitionPath1, Arrays.asList(fileId1));
    metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime2), Option.empty());
    writeClient.commit(instantTime2, rdd2, Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION, partitionToReplacedFileIds);

    assertFileGroupCorrectness(instantTime2, partitionPath1, filePath2, fileId2, 1);

    // Add file to a new partition
    String partitionPath2 = partitions.get(1);
    String instantTime3 = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, metaClient);
    Pair<String, String> fileIdAndName3 = fileIdAndNameGenerator.generate(3, instantTime3);
    String fileId3 = fileIdAndName3.getLeft();
    String fileName3 = fileIdAndName3.getRight();
    String filePath3 = getPath(partitionPath2, fileName3);
    WriteStatus writeStatus3 = createWriteStatus(partitionPath2, filePath3, fileId3);
    JavaRDD<WriteStatus> rdd3 = createRdd(Arrays.asList(writeStatus3));
    metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime3), Option.empty());
    writeClient.commit(instantTime3, rdd3, Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION, Collections.emptyMap());

    assertFileGroupCorrectness(instantTime3, partitionPath2, filePath3, fileId3, partitionPath2.isEmpty() ? 2 : 1);

    // clean first commit
    String cleanTime = HoodieActiveTimeline.createNewInstantTime();
    HoodieCleanerPlan cleanerPlan = cleanerPlan(new HoodieActionInstant(instantTime2, HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieInstant.State.COMPLETED.name()), instantTime3,
        Collections.singletonMap(partitionPath1, Arrays.asList(new HoodieCleanFileInfo(filePath1, false))));
    metaClient.getActiveTimeline().saveToCleanRequested(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanTime),
        TimelineMetadataUtils.serializeCleanerPlan(cleanerPlan));
    HoodieInstant inflightClean = metaClient.getActiveTimeline().transitionCleanRequestedToInflight(
        new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanTime), Option.empty());
    List<HoodieCleanStat> cleanStats = Arrays.asList(createCleanStat(partitionPath1, Arrays.asList(filePath1), instantTime2, instantTime3));
    HoodieCleanMetadata metadata = CleanerUtils.convertCleanMetadata(
        cleanTime,
        Option.empty(),
        cleanStats
    );
    HoodieTableMetadataWriter hoodieTableMetadataWriter = (HoodieTableMetadataWriter) writeClient.initTable(WriteOperationType.UPSERT, Option.of(cleanTime)).getMetadataWriter(cleanTime).get();
    hoodieTableMetadataWriter.update(metadata, cleanTime);
    metaClient.getActiveTimeline().transitionCleanInflightToComplete(inflightClean,
        TimelineMetadataUtils.serializeCleanMetadata(metadata));
    // make sure we still get the same results as before
    assertFileGroupCorrectness(instantTime2, partitionPath1, filePath2, fileId2, partitionPath2.isEmpty() ? 2 : 1);
    assertFileGroupCorrectness(instantTime3, partitionPath2, filePath3, fileId3, partitionPath2.isEmpty() ? 2 : 1);

    // trigger archiver manually
    writeClient.archive();
    // assert commit was archived
    Assertions.assertEquals(1, metaClient.getArchivedTimeline().reload().filterCompletedInstants().countInstants());
    // make sure we still get the same results as before
    assertFileGroupCorrectness(instantTime2, partitionPath1, filePath2, fileId2, partitionPath2.isEmpty() ? 2 : 1);
    assertFileGroupCorrectness(instantTime3, partitionPath2, filePath3, fileId3, partitionPath2.isEmpty() ? 2 : 1);
  }

  static Stream<Arguments> getArgs() {
    FileIdAndNameGenerator hudiBased = (index, instantTime) -> {
      String fileId = UUID.randomUUID().toString();
      String fileName = String.format("%s_0-28-34_%s.parquet", fileId, instantTime);
      return Pair.of(fileId, fileName);
    };
    FileIdAndNameGenerator external = (index, instantTime) -> {
      String fileName = String.format("file_%d.parquet", index);
      String fileId = fileName;
      return Pair.of(fileId, fileName);
    };
    List<String> partitionedTable = Arrays.asList("americas/brazil", "americas/argentina");
    List<String> unpartitionedTable = Arrays.asList("", "");
    return Stream.of(Arguments.of(hudiBased, partitionedTable), Arguments.of(external, partitionedTable), Arguments.of(hudiBased, unpartitionedTable), Arguments.of(external, unpartitionedTable));
  }

  private String getPath(String partitionPath, String fileName) {
    if (partitionPath.isEmpty()) {
      return fileName;
    }
    return String.format("%s/%s", partitionPath, fileName);
  }

  @FunctionalInterface
  private interface FileIdAndNameGenerator {
    Pair<String, String> generate(int iteration, String instantTime);
  }

  private void assertFileGroupCorrectness(String instantTime, String partitionPath, String filePath, String fileId, int expectedSize) {
    HoodieTableFileSystemView fsView = new HoodieMetadataFileSystemView(context, metaClient, metaClient.reloadActiveTimeline(), writeConfig.getMetadataConfig());
    List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());
    Assertions.assertEquals(expectedSize, fileGroups.size());
    Option<HoodieFileGroup> fileGroupOption = Option.fromJavaOptional(fileGroups.stream().filter(fg -> fg.getFileGroupId().getFileId().equals(fileId)).findFirst());
    Assertions.assertTrue(fileGroupOption.isPresent());
    HoodieFileGroup fileGroup = fileGroupOption.get();
    Assertions.assertEquals(fileId, fileGroup.getFileGroupId().getFileId());
    Assertions.assertEquals(partitionPath, fileGroup.getPartitionPath());
    HoodieBaseFile baseFile = fileGroup.getAllBaseFiles().findFirst().get();
    Assertions.assertEquals(instantTime, baseFile.getCommitTime());
    Assertions.assertEquals(metaClient.getBasePathV2().toString() + "/" + filePath, baseFile.getPath());
  }

  private JavaRDD<WriteStatus> createRdd(List<WriteStatus> writeStatuses) {
    Dataset<WriteStatus> dsw = sparkSession.createDataset(writeStatuses, Encoders.javaSerialization(WriteStatus.class));
    return new JavaRDD<>(dsw.rdd(), ClassTag.apply(WriteStatus.class));
  }

  private WriteStatus createWriteStatus(String partitionPath, String filePath, String fileId) {
    WriteStatus writeStatus = new WriteStatus();
    writeStatus.setFileId(fileId);
    writeStatus.setPartitionPath(partitionPath);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(fileId);
    writeStat.setPath(filePath);
    writeStat.setPartitionPath(partitionPath);
    writeStat.setNumWrites(3);
    writeStat.setNumDeletes(0);
    writeStat.setNumUpdateWrites(0);
    writeStat.setNumInserts(3);
    writeStat.setTotalWriteBytes(400);
    writeStat.setTotalWriteErrors(0);
    writeStat.setFileSizeInBytes(400);
    writeStat.setTotalLogBlocks(0);
    writeStat.setTotalLogRecords(0);
    writeStat.setTotalLogFilesCompacted(0);
    writeStat.setTotalLogSizeCompacted(0);
    writeStat.setTotalUpdatedRecordsCompacted(0);
    writeStat.setTotalCorruptLogBlock(0);
    writeStat.setTotalRollbackBlocks(0);
    writeStatus.setStat(writeStat);
    return writeStatus;
  }

  private HoodieCleanStat createCleanStat(String partitionPath, List<String> deletePaths, String earliestCommitToRetain, String lastCompletedCommitTimestamp) {
    // TODO is path supposed to be the file name or include the partition path? Is it consistent between commit types?
    List<String> pathsWithoutPartition = deletePaths.stream().map(path -> path.substring(partitionPath.length() + 1)).collect(Collectors.toList());
    return new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_COMMITS, partitionPath, pathsWithoutPartition, pathsWithoutPartition, Collections.emptyList(),
        earliestCommitToRetain, lastCompletedCommitTimestamp);
  }

  private HoodieCleanerPlan cleanerPlan(HoodieActionInstant earliestInstantToRetain, String latestCommit, Map<String, List<HoodieCleanFileInfo>> filePathsToBeDeletedPerPartition) {
    return new HoodieCleanerPlan(earliestInstantToRetain,
        latestCommit,
        writeConfig.getCleanerPolicy().name(), Collections.emptyMap(),
        CleanPlanner.LATEST_CLEAN_PLAN_VERSION, filePathsToBeDeletedPerPartition, Collections.emptyList());
  }
}
