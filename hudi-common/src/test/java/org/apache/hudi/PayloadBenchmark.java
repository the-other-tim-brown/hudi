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

package org.apache.hudi;

import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Baseline:
 * PayloadBenchmark.fieldAccessTest  thrpt   25  2236058.600 ± 14359.231  ops/s
 * New - Round1:
 * PayloadBenchmark.fieldAccessTest  thrpt   25  2171138.045 ± 14191.624  ops/s ~3% slower
 * New - avoid casting?
 * PayloadBenchmark.fieldAccessTest  thrpt   25  2258774.303 ± 17682.837  ops/s - on par
 *
 * PayloadBenchmark.serdePerf         thrpt   25  25544.914 ± 256.355  ops/s
 * PayloadBenchmark.serdePerfPayload  thrpt   25  13202.017 ± 225.340  ops/s
 */
public class PayloadBenchmark {
  private static final Random RANDOM = new Random(0);
  private static final Properties PROPERTIES = new Properties();

  private static final Schema SCHEMA = SchemaBuilder.record("test_record")
      .fields()
      .name("field1").type().intType().noDefault()
      .name("field2").type().stringType().noDefault()
      .name("field3").type().booleanType().noDefault()
      .name("field4").type().doubleType().noDefault()
      .name("field5").type().floatType().noDefault()
      .name("field6").type().longType().noDefault()
      .name("field7").type().bytesType().noDefault()
      .name("field8").type().array().items().intType().noDefault()
      .name("field9").type().map().values().stringType().noDefault()
      .name("field10").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
      .name("field11").type().unionOf().nullType().and().intType().endUnion().nullDefault()
      .name("field12").type().unionOf().nullType().and().booleanType().endUnion().nullDefault()
      .name("field13").type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
      .name("field14").type().unionOf().nullType().and().floatType().endUnion().nullDefault()
      .name("field15").type().unionOf().nullType().and().longType().endUnion().nullDefault()
      .endRecord();

  // field indexes in random order
  private static final List<Integer> FIELD_ORDER = Arrays.asList(10, 1, 3, 11, 7, 6, 2, 12, 4, 5, 13, 14, 0, 8, 9);

  private static final List<IndexedRecord> BASELINE_RECORDS = IntStream.range(0, 100)
      .mapToObj(i -> createRecord())
      .collect(Collectors.toList());

  private static final List<HoodieAvroIndexedRecord> TEST_RECORDS = BASELINE_RECORDS.stream()
      .map(HoodieAvroIndexedRecord::new)
      .collect(Collectors.toList());

  private static final List<HoodieAvroRecord> PAYLOAD_RECORDS = BASELINE_RECORDS.stream()
      .map(indexedRecord -> new HoodieAvroRecord(null, new DefaultHoodieRecordPayload(Option.of((GenericRecord) indexedRecord))))
      .collect(Collectors.toList());


//  @Benchmark
//  public void fieldAccessTest(Blackhole blackhole) {
//    for (HoodieAvroIndexedRecord record : TEST_RECORDS) {
//      blackhole.consume(record.toIndexedRecord(SCHEMA, PROPERTIES));
//    }
//  }

  @Benchmark
  public void serdePerf(Blackhole blackhole) throws Exception {
    for (HoodieAvroIndexedRecord record : TEST_RECORDS) {
      byte[] bytes = SerializationUtils.serialize(record);
      blackhole.consume(bytes);
      HoodieAvroIndexedRecord deserializedRecord = SerializationUtils.deserialize(bytes);
      blackhole.consume(deserializedRecord);
    }
  }

  @Benchmark
  public void serdePerfPayload(Blackhole blackhole) throws Exception {
    for (HoodieAvroRecord record : PAYLOAD_RECORDS) {
      byte[] bytes = SerializationUtils.serialize(record);
      blackhole.consume(bytes);
      HoodieAvroRecord deserializedRecord = SerializationUtils.deserialize(bytes);
      blackhole.consume(deserializedRecord);
    }
  }

  @Test
  void sizeTest() throws Exception {
    long totalSize = 0;
    long totalSizePayload = 0;
    for (HoodieAvroIndexedRecord record : TEST_RECORDS) {
      byte[] bytes = SerializationUtils.serialize(record);
      totalSize += bytes.length;
      HoodieAvroIndexedRecord deserializedRecord = SerializationUtils.deserialize(bytes);
    }

    for (HoodieAvroRecord record : PAYLOAD_RECORDS) {
      byte[] bytes = SerializationUtils.serialize(record);
      totalSizePayload += bytes.length;
      HoodieAvroRecord deserializedRecord = SerializationUtils.deserialize(bytes);
    }

    System.out.println("Total size of HoodieAvroIndexedRecord: " + totalSize);
    System.out.println("Total size of HoodieAvroRecord: " + totalSizePayload);
    System.out.println("Size difference: " + (totalSize - totalSizePayload));
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(PayloadBenchmark.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }

  private static IndexedRecord createRecord() {
    IndexedRecord record = new GenericData.Record(SCHEMA);
    record.put(0, RANDOM.nextInt());
    record.put(1, "test");
    record.put(2, RANDOM.nextBoolean());
    record.put(3, RANDOM.nextDouble());
    record.put(4, RANDOM.nextFloat());
    record.put(5, RANDOM.nextLong());
    record.put(6, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    record.put(7, Arrays.asList(RANDOM.nextInt(), RANDOM.nextInt()));
    record.put(8, Collections.singletonMap("key1", "value1"));
    record.put(9, RANDOM.nextBoolean() ? null : "optional string");
    record.put(10, RANDOM.nextBoolean() ? null : RANDOM.nextInt());
    record.put(11, RANDOM.nextBoolean() ? null : RANDOM.nextBoolean());
    record.put(12, RANDOM.nextBoolean() ? null : RANDOM.nextDouble());
    record.put(13, RANDOM.nextBoolean() ? null : RANDOM.nextFloat());
    record.put(14, RANDOM.nextBoolean() ? null : RANDOM.nextLong());
    return record;
  }
}
