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

package org.apache.spark.sql.hudi.feature.index

import org.apache.hudi.{DataSourceReadOptions, ExpressionIndexSupport, HoodieFileIndex, HoodieSparkUtils, SparkAdapterSupport}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.avro.model.HoodieMetadataBloomFilter
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.utils.SparkMetadataWriterUtils
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieIndexDefinition}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.view.{FileSystemViewManager, HoodieTableFileSystemView}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.{HoodieCleanConfig, HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.index.expression.HoodieExpressionIndex
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieIndexVersion, HoodieMetadataPayload, MetadataPartitionType}
import org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionStatsIndexKey
import org.apache.hudi.stats.{SparkValueMetadataUtils, ValueType}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient
import org.apache.hudi.util.JFunction

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{functions, Column, SaveMode}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.resolveExpr
import org.apache.spark.sql.catalyst.analysis.{Analyzer, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, FromUnixTime, Literal, Upper}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hudi.command.{CreateIndexCommand, ShowIndexesCommand}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import java.util.stream.Collectors

import scala.collection.JavaConverters

class TestVectorIndex extends HoodieSparkSqlTestBase with SparkAdapterSupport {

  override protected def beforeAll(): Unit = {
    spark.sql("set hoodie.metadata.index.column.stats.enable=false")
    spark.sql("set hoodie.write.lock.provider = org.apache.hudi.client.transaction.lock.InProcessLockProvider")
    spark.sparkContext.persistentRdds.foreach(rdd => rdd._2.unpersist())
    initQueryIndexConf()
  }

  test("Test Create Vector Index Syntax") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val databaseName = "default"
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  embedding ARRAY<float>
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = '$tableType'
             | )
             | location '$basePath'
       """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, ARRAY(0.1, 0.2, 0.3))")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, ARRAY(0.1, 0.2, 0.3))")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, ARRAY(0.1, 0.2, 0.3))")

        val sqlParser: ParserInterface = spark.sessionState.sqlParser
        val analyzer: Analyzer = spark.sessionState.analyzer

        var logicalPlan = sqlParser.parsePlan(s"show indexes from default.$tableName")
        var resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[ShowIndexesCommand].table, databaseName, tableName)

        logicalPlan = sqlParser.parsePlan(s"create index idx_embedding on $tableName using vector_index(embedding)")
        resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
        assertResult("idx_embedding")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
        assertResult("vector_index")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
        assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)
      }
    }
  }

  test("Test Create and Drop Vector Index") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val databaseName = "default"
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  embedding1 array<float>,
             |  embedding2 array<float>
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  hoodie.metadata.record.index.enable = 'true',
             |  hoodie.datasource.write.recordkey.field = 'id'
             | )
             | location '$basePath'
       """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, ARRAY(0.1, 0.2, 0.3), ARRAY(0.1, 0.2, 0.3))")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, ARRAY(0.1, 0.2, 0.3), ARRAY(0.1, 0.2, 0.3))")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, ARRAY(0.1, 0.2, 0.3), ARRAY(0.1, 0.2, 0.3))")

        var metaClient = createMetaClient(spark, basePath)

        assert(metaClient.getTableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX))

        val sqlParser: ParserInterface = spark.sessionState.sqlParser
        val analyzer: Analyzer = spark.sessionState.analyzer

        var logicalPlan = sqlParser.parsePlan(s"show indexes from default.$tableName")
        var resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[ShowIndexesCommand].table, databaseName, tableName)

        var createIndexSql = s"create index idx_embedding on $tableName using vector_index(embedding1)"
        logicalPlan = sqlParser.parsePlan(createIndexSql)

        resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
        assertResult("idx_embedding")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
        assertResult("vector_index")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
        assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)

        spark.sql(createIndexSql)
        metaClient = createMetaClient(spark, basePath)
        assertTrue(metaClient.getIndexMetadata.isPresent)
        var expressionIndexMetadata = metaClient.getIndexMetadata.get()
        // RLI and expression index
        assertEquals(2, expressionIndexMetadata.getIndexDefinitions.size())
        assertEquals("vector_index_idx_embedding", expressionIndexMetadata.getIndexDefinitions.get("vector_index_idx_embedding").getIndexName)

        // Verify one can create more than one vector index on different columns
        createIndexSql = s"create index idx_vec_2 on $tableName using vector_index(embedding2)"
        spark.sql(createIndexSql)
        metaClient = createMetaClient(spark, basePath)
        expressionIndexMetadata = metaClient.getIndexMetadata.get()
        // RLI and 2 expression indexes
        assertEquals(3, expressionIndexMetadata.getIndexDefinitions.size())
        assertEquals("vector_index_idx_vec_2", expressionIndexMetadata.getIndexDefinitions.get("vector_index_idx_vec_2").getIndexName)

        // Ensure that both the indexes are tracked correctly in metadata partition config
        val mdtPartitions = metaClient.getTableConfig.getMetadataPartitions
        assert(mdtPartitions.contains("vector_index_idx_vec_2") && mdtPartitions.contains("vector_index_idx_embedding"))

        assert(metaClient.getTableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX))

        // drop expression index
        spark.sql(s"drop index vector_index_idx_embedding on $tableName")
        // validate table config
        metaClient = HoodieTableMetaClient.reload(metaClient)
        assert(!metaClient.getTableConfig.getMetadataPartitions.contains("vector_index_idx_embedding"))
        assert(metaClient.getTableConfig.getMetadataPartitions.contains("vector_index_idx_vec_2"))
      }
    }
  }

  test("Test vector index update after initialization") {
    withTempDir(tmp => {
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""create table $tableName (
            id int,
            name string,
            price double,
            embedding ARRAY<float>
            ) using hudi
            options (
            primaryKey ='id',
            type = 'mor',
            hoodie.metadata.record.index.enable = 'true',
            hoodie.datasource.write.recordkey.field = 'id'
            )
            location '$basePath'""".stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, ARRAY(0.1, 0.2, 0.3))")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, ARRAY(0.1, 0.2, 0.3))")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, ARRAY(0.1, 0.2, 0.3))")

      // create vector index
      val createIndexSql = s"create index idx_embedding on $tableName using vector_index(embedding)"
      spark.sql(createIndexSql)
      val metaClient = createMetaClient(spark, basePath)
      val expressionIndexMetadata = metaClient.getIndexMetadata.get()
      // RLI and expression indexes
      assertEquals(2, expressionIndexMetadata.getIndexDefinitions.size())
      assertEquals("vector_index_idx_embedding", expressionIndexMetadata.getIndexDefinitions.get("vector_index_idx_embedding").getIndexName)
      assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("vector_index_idx_embedding"))
      assertTrue(metaClient.getIndexMetadata.isPresent)

      // do another insert after initializing the index
      spark.sql(s"insert into $tableName values(4, 'a4', 10, ARRAY(0.1, 0.2, 0.3))")

      // update a record
      spark.sql(s"update $tableName set name = 'a1_updated', embedding = ARRAY(0.4, 0.5, 0.6) where id = 1")

      // TODO validate the index is updated
    })
  }

  private def assertTableIdentifier(catalogTable: CatalogTable,
                                    expectedDatabaseName: String,
                                    expectedTableName: String): Unit = {
    assertResult(Some(expectedDatabaseName))(catalogTable.identifier.database)
    assertResult(expectedTableName)(catalogTable.identifier.table)
  }
}
