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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TimelineLayout}.
 */
public class TestTimelineLayout  {

  @Test
  public void testTimelineLayoutFilter() {
    List<HoodieInstant> rawInstants = Arrays.asList(
        INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "001"),
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "001"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "001"),
        INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, "002"),
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "002"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "002"),
        INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "003"),
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "003"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "003"),
        INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "004"),
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "004"),
        INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, "005"),
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "005"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "005"),
        INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "006"),
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "006"),
        INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, "007"),
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "007"));

    List<HoodieInstant> layout0Instants = TimelineLayout.fromVersion(new TimelineLayoutVersion(0))
        .filterHoodieInstants(rawInstants.stream()).collect(Collectors.toList());
    assertEquals(rawInstants, layout0Instants);
    List<HoodieInstant> layout1Instants = TimelineLayout.fromVersion(TimelineLayoutVersion.CURR_LAYOUT_VERSION)
        .filterHoodieInstants(rawInstants.stream()).collect(Collectors.toList());
    assertEquals(7, layout1Instants.size());
    assertTrue(layout1Instants.contains(
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "007")));
    assertTrue(layout1Instants.contains(
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "006")));
    assertTrue(layout1Instants.contains(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "005")));
    assertTrue(layout1Instants.contains(
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "004")));
    assertTrue(layout1Instants.contains(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "003")));
    assertTrue(layout1Instants.contains(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "002")));
    assertTrue(layout1Instants.contains(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "001")));
  }
}
