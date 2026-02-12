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

import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hudi.blob.BatchedByteRangeReader.DATA_COL
import org.apache.spark.sql.hudi.expressions.ResolveBytesExpression
import org.apache.spark.sql.types.BinaryType
import org.slf4j.LoggerFactory

/**
 * Analyzer rule to resolve blob references efficiently using BatchedByteRangeReader.
 *
 * This rule detects queries containing [[ResolveBytesExpression]] markers (created by
 * the `resolve_bytes()` SQL function) and transforms the logical plan to use batched
 * I/O operations for efficient blob data reading.
 *
 * <h3>Transformation Process:</h3>
 * {{{
 * Before: Project([id, resolve_bytes(file_info) as data], child)
 * After:  Project([id, __temp__data as data], BatchedRead(child))
 * }}}
 *
 * <h3>How It Works:</h3>
 * <ol>
 *   <li>Detects [[Project]] nodes containing [[ResolveBytesExpression]] markers</li>
 *   <li>Applies [[BatchedByteRangeReader.readBatched()]] to the child DataFrame</li>
 *   <li>Replaces [[ResolveBytesExpression]] with references to the `__temp__data` column</li>
 *   <li>Returns transformed plan with efficient batched I/O</li>
 * </ol>
 *
 * <h3>Configuration:</h3>
 * <ul>
 *   <li>`hoodie.blob.batching.max.gap.bytes` (default: 4096) - Max gap between reads to batch</li>
 *   <li>`hoodie.blob.batching.lookahead.size` (default: 50) - Rows to buffer for batch detection</li>
 * </ul>
 *
 * <h3>Performance:</h3>
 * <ul>
 *   <li>2-5x speedup for sorted data (by reference.file, reference.position)</li>
 *   <li>Reduces file seeks by merging consecutive reads</li>
 *   <li>Configurable batching parameters for different workloads</li>
 * </ul>
 *
 * @param spark SparkSession for accessing configuration
 */
case class ResolveBlobReferencesRule(spark: SparkSession) extends Rule[LogicalPlan] {

  private val logger = LoggerFactory.getLogger(classOf[ResolveBlobReferencesRule])

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case _ @ Project(projectList, child) if containsResolveBytesExpression(projectList) =>
      transformProjectWithBlobResolution(projectList, child)
  }

  /**
   * Check if any expression in the project list contains a ResolveBytesExpression.
   */
  private def containsResolveBytesExpression(projectList: Seq[Expression]): Boolean = {
    projectList.exists(expr => containsResolveBytesInExpression(expr))
  }

  private def containsResolveBytesInExpression(expr: Expression): Boolean = {
    expr match {
      case _: ResolveBytesExpression => true
      case other => other.children.exists(containsResolveBytesInExpression)
    }
  }

  /**
   * Transform the project to use lazy BatchedBlobRead node.
   *
   * Creates a double-Project plan to prevent column pruning:
   * <ol>
   *   <li>Inner Project: Includes blob column + data column (prevents pruning)</li>
   *   <li>Outer Project: User's requested columns only</li>
   * </ol>
   */
  private def transformProjectWithBlobResolution(
      projectList: Seq[NamedExpression],
      child: LogicalPlan): LogicalPlan = {

    // Get configuration
    val storageConf = new HadoopStorageConfiguration(spark.sparkContext.hadoopConfiguration)
    val maxGapBytes = spark.conf.get("hoodie.blob.batching.max.gap.bytes", "4096").toInt
    val lookaheadSize = spark.conf.get("hoodie.blob.batching.lookahead.size", "50").toInt

    // Extract blob column name
    val blobColumnName = extractBlobColumnNameFromExpressions(projectList)

    // Create data attribute for the generated column
    val dataAttr = AttributeReference(DATA_COL, BinaryType, nullable = false)()

    // Create blob input expression to prevent pruning
    val blobInputExprs = blobColumnName.flatMap { colName =>
      child.output.find(_.name == colName).map(attr => BlobInputMarker(attr))
    }.toSeq

    // Create BatchedBlobRead node (LAZY - no I/O yet)
    val batchedChild = BatchedBlobRead(
      child,
      blobColumnName,
      maxGapBytes,
      lookaheadSize,
      storageConf,
      dataAttr,
      blobInputExprs
    )

    // Transform project list: replace resolve_bytes() with dataAttr BUT keep blob column
    val transformedProjectList = projectList.flatMap {
      case alias @ Alias(ResolveBytesExpression(blobRef: AttributeReference), name) =>
        // Return BOTH the blob column and the data column
        Seq(
          blobRef,  // Keep blob column reference to prevent pruning
          Alias(dataAttr, name)(alias.exprId, alias.qualifier, alias.explicitMetadata)
        )
      case attr: AttributeReference =>
        Seq(attr)  // Keep other columns
      case other =>
        Seq(other)  // Keep other expressions
    }

    // Create inner project with blob column preserved
    val innerProject = Project(transformedProjectList, batchedChild)

    // Create outer project with only user-requested columns
    val finalProjectList = projectList.map {
      case alias @ Alias(ResolveBytesExpression(_), name) =>
        // Reference the data column from inner project
        val dataRef = innerProject.output.find(_.name == name).getOrElse {
          throw new RuntimeException(s"Expected data column '$name' not found in inner project")
        }
        Alias(dataRef, name)(alias.exprId, alias.qualifier, alias.explicitMetadata)
      case attr: AttributeReference =>
        innerProject.output.find(_.name == attr.name).getOrElse(attr)
      case other =>
        other
    }

    Project(finalProjectList, innerProject)
  }


  /**
   * Extract blob column name from expressions (handles nesting in aggregate functions).
   */
  private def extractBlobColumnNameFromExpressions(expressions: Seq[Expression]): Option[String] = {
    expressions.collectFirst {
      case expr if containsResolveBytesInExpression(expr) =>
        findBlobColumnName(expr)
    }.flatten
  }

  /**
   * Recursively search for ResolveBytesExpression and extract column name.
   */
  private def findBlobColumnName(expr: Expression): Option[String] = expr match {
    case ResolveBytesExpression(child) =>
      child match {
        case attr: AttributeReference => Some(attr.name)
        case _ => None
      }
    case other =>
      other.children.flatMap(findBlobColumnName).headOption
  }
}
