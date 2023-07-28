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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import scala.reflect.ClassTag;

import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;

public class TestExternalPaths extends HoodieClientTestBase {
  @Test
  public void testFlow() throws Exception {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(INMEMORY).build())
        .withPath(metaClient.getBasePathV2().toString())
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .build();

    writeClient = new SparkRDDWriteClient(context, writeConfig, false, Option.empty());
    String instantTime = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, metaClient);
    WriteStatus writeStatus = new WriteStatus();
    String fileId = UUID.randomUUID().toString();
    writeStatus.setFileId(fileId);
    String partitionPath = "/americas/brazil";
    writeStatus.setPartitionPath(partitionPath);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(fileId);
    writeStat.setPath("/americas/brazil/5d54b5e3-d612-42ee-9656-32f22c83233b-0_0-28-34_20220804164034570.parquet");
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
    List<WriteStatus> writeStatuses = Arrays.asList(writeStatus);
    Dataset<WriteStatus> dsw = sparkSession.createDataset(writeStatuses, Encoders.javaSerialization(WriteStatus.class));
    JavaRDD<WriteStatus> rdd = new JavaRDD<>(dsw.rdd(), ClassTag.apply(WriteStatus.class));
    Option<Map<String, String>> extraMetadata = Option.empty();
    String commitActionType = HoodieTimeline.REPLACE_COMMIT_ACTION;
    Map<String, List<String>> partitionToReplacedFileIds = Collections.emptyMap();
    metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime), Option.empty());
    writeClient.commit(instantTime, rdd, extraMetadata, commitActionType, partitionToReplacedFileIds);

    HoodieTableFileSystemView fsView = new HoodieMetadataFileSystemView(context, metaClient, metaClient.getActiveTimeline(), writeConfig.getMetadataConfig());
    fsView.init(metaClient, metaClient.getActiveTimeline());
    List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());
    Assertions.assertEquals(1, fsView.getAllFileGroups().collect(Collectors.toList()).size());
  }
}
