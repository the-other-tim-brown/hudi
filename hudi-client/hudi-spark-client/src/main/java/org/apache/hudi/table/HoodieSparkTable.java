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

package org.apache.hudi.table;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.SparkPartitionUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.SparkHoodieIndexFactory;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.commit.HoodieMergeHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;

import java.io.IOException;

public abstract class HoodieSparkTable<T>
    extends HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> {

  private volatile boolean isMetadataTableExists = false;

  protected HoodieSparkTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  public static <T> HoodieSparkTable<T> create(HoodieWriteConfig config, HoodieEngineContext context) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(context.getStorageConf().newInstance())
            .setBasePath(config.getBasePath())
            .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
            .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion())))
            .setTimeGeneratorConfig(config.getTimeGeneratorConfig())
            .setFileSystemRetryConfig(config.getFileSystemRetryConfig())
            .setMetaserverConfig(config.getProps()).build();
    return HoodieSparkTable.create(config, context, metaClient);
  }

  public static <T> HoodieSparkTable<T> create(HoodieWriteConfig config,
                                               HoodieEngineContext context,
                                               HoodieTableMetaClient metaClient) {
    HoodieSparkTable<T> hoodieSparkTable;
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        hoodieSparkTable = new HoodieSparkCopyOnWriteTable<>(config, context, metaClient);
        break;
      case MERGE_ON_READ:
        hoodieSparkTable = new HoodieSparkMergeOnReadTable<>(config, context, metaClient);
        break;
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
    return hoodieSparkTable;
  }

  @Override
  protected HoodieIndex getIndex(HoodieWriteConfig config, HoodieEngineContext context) {
    return SparkHoodieIndexFactory.createIndex(config);
  }

  /**
   * Fetch instance of {@link HoodieTableMetadataWriter}.
   *
   * @return instance of {@link HoodieTableMetadataWriter}
   */
  @Override
  protected Option<HoodieTableMetadataWriter> getMetadataWriter(
      String triggeringInstantTimestamp,
      HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy) {
    if (config.isMetadataTableEnabled()) {
      // if any partition is deleted, we need to reload the metadata table writer so that new table configs are picked up
      // to reflect the delete mdt partitions.
      deleteMetadataIndexIfNecessary();

      // Create the metadata table writer. First time after the upgrade this creation might trigger
      // metadata table bootstrapping. Bootstrapping process could fail and checking the table
      // existence after the creation is needed.
      HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(
          context.getStorageConf(), config, failedWritesCleaningPolicy, context,
          Option.of(triggeringInstantTimestamp));
      try {
        if (isMetadataTableExists || metaClient.getStorage().exists(new StoragePath(
            HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath())))) {
          isMetadataTableExists = true;
          return Option.of(metadataWriter);
        }
      } catch (IOException e) {
        throw new HoodieMetadataException("Checking existence of metadata table failed", e);
      }
    } else {
      // if metadata is not enabled in the write config, we should try and delete it (if present)
      maybeDeleteMetadataTable();
    }

    return Option.empty();
  }

  @Override
  public Runnable getPreExecuteRunnable() {
    final TaskContext taskContext = TaskContext.get();
    return () -> TaskContext$.MODULE$.setTaskContext(taskContext);
  }

  @Override
  public void runMerge(HoodieMergeHandle<?, ?, ?, ?> upsertHandle, String instantTime, String fileId) throws IOException {
    if (upsertHandle.getOldFilePath() == null) {
      throw new HoodieUpsertException("Error in finding the old file path at commit " + instantTime + " for fileId: " + fileId);
    } else {
      if (upsertHandle.baseFileForMerge().getBootstrapBaseFile().isPresent()) {
        Option<String[]> partitionFields = getMetaClient().getTableConfig().getPartitionFields();
        Object[] partitionValues = SparkPartitionUtils.getPartitionFieldVals(partitionFields, upsertHandle.getPartitionPath(),
            getMetaClient().getTableConfig().getBootstrapBasePath().get(),
            upsertHandle.getWriterSchema(), getStorageConf().unwrapAs(Configuration.class));
        upsertHandle.setPartitionFields(partitionFields);
        upsertHandle.setPartitionValues(partitionValues);
      }
      HoodieMergeHelper.newInstance().runMerge(this, upsertHandle);
    }
  }

}
