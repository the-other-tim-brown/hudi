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

package org.apache.spark.sql.hudi.blob

import org.apache.hudi.HoodieDatasetBulkInsertHelper.sparkAdapter
import org.apache.hudi.storage.StorageConfiguration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}


/**
 * Physical operator for batched blob reading.
 *
 * Executes the batched I/O operations to read blob data efficiently.
 * This operator implements the physical execution for [[BatchedBlobRead]]
 * logical plan nodes.
 *
 * @param child The child physical plan
 * @param childLogicalPlan The child's logical plan (needed for DataFrame creation)
 * @param blobColumnName Optional name of the column containing blob references
 * @param maxGapBytes Maximum gap between reads to batch together
 * @param lookaheadSize Number of rows to buffer for batch detection
 * @param storageConf Storage configuration for reading files
 * @param dataAttribute The generated data attribute - passed from logical plan for ExprId stability
 */
case class BatchedBlobReadExec(
    child: SparkPlan,
    childLogicalPlan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
    blobColumnName: Option[String],
    maxGapBytes: Int,
    lookaheadSize: Int,
    storageConf: StorageConfiguration[_],
    dataAttribute: Attribute
) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output :+ dataAttribute

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    println(s"[DEBUG] BatchedBlobReadExec.doExecute - child.schema=${child.schema.fieldNames.mkString(",")}")
    println(s"[DEBUG] BatchedBlobReadExec.doExecute - blobColumnName=$blobColumnName")
    println(s"[DEBUG] BatchedBlobReadExec.doExecute - childLogicalPlan.output=${childLogicalPlan.output.map(_.name).mkString(",")}")

    // Get SparkSession from the session state
    val spark = org.apache.spark.sql.SparkSession.active

    // Create a DataFrame from the child logical plan (lazy - doesn't execute)
    val childDF = sparkAdapter.getUnsafeUtils.createDataFrameFrom(spark, childLogicalPlan)
    println(s"[DEBUG] BatchedBlobReadExec.doExecute - childDF.schema=${childDF.schema.fieldNames.mkString(",")}")

    // Apply BatchedByteRangeReader (lazy - returns a DataFrame, doesn't execute)
    val resultDF = BatchedByteRangeReader.readBatched(
      childDF,
      storageConf,
      maxGapBytes,
      lookaheadSize,
      blobColumnName,
      keepTempColumn = true
    )

    // Return the RDD from the DataFrame's query plan
    // The actual I/O happens when this RDD is computed
    resultDF.queryExecution.toRdd
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
