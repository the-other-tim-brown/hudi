/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.{HoodieStorage, StoragePath}

import scala.jdk.CollectionConverters.asScalaBufferConverter

/**
 * TODO convert to Java, move to hudi-common
 */
object PathUtils {

  /**
   * This method copied from [[org.apache.spark.deploy.SparkHadoopUtil]].
   * [[org.apache.spark.deploy.SparkHadoopUtil]] becomes private since Spark 3.0.0 and hence we had to copy it locally.
   */
  def isGlobPath(pattern: StoragePath): Boolean = {
    pattern.toString.exists("{}[]*?\\".toSet.contains)
  }

  /**
   * This method is inspired from [[org.apache.spark.deploy.SparkHadoopUtil]] with some modifications like
   * skipping meta paths.
   */
  def globPath(storage: HoodieStorage, pattern: StoragePath): Seq[StoragePath] = {
    // find base path to assist in skipping meta paths
    var basePath = pattern.getParent
    while (basePath.getName.equals("*")) {
      basePath = basePath.getParent
    }

    Option(storage.globEntries(pattern)).map { pathInfoList => {
      val nonMetaStatuses = pathInfoList.asScala.filterNot(entry => {
        // skip all entries in meta path
        var leafPath = entry.getPath
        // walk through every parent until we reach base path. if .hoodie is found anywhere, path needs to be skipped
        while (!leafPath.equals(basePath) && !leafPath.getName.equals(HoodieTableMetaClient.METAFOLDER_NAME)) {
          leafPath = leafPath.getParent
        }
        leafPath.getName.equals(HoodieTableMetaClient.METAFOLDER_NAME)
      })
      nonMetaStatuses.map(e => e.getPath.makeQualified(storage.getUri))
    }
    }.getOrElse(Seq.empty[StoragePath])
  }

  /**
   * This method copied from [[org.apache.spark.deploy.SparkHadoopUtil]].
   * [[org.apache.spark.deploy.SparkHadoopUtil]] becomes private since Spark 3.0.0 and hence we had to copy it locally.
   */
  def globPathIfNecessary(storage: HoodieStorage, pattern: StoragePath): Seq[StoragePath] = {
    if (isGlobPath(pattern)) globPath(storage, pattern) else Seq(pattern)
  }

  /**
   * Checks to see whether input path contains a glob pattern and if yes, maps it to a list of absolute paths
   * which match the glob pattern. Otherwise, returns original path
   *
   * @param paths List of absolute or globbed paths
   * @param fs    {@link HoodieStorage} instance
   * @return list of absolute file paths
   */
  def checkAndGlobPathIfNecessary(paths: Seq[String], storage: HoodieStorage): Seq[StoragePath] = {
    paths.flatMap(path => {
      val qualified = new StoragePath(path).makeQualified(storage.getUri);
      globPathIfNecessary(storage, qualified)
    })
  }
}
