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

package org.apache.hudi.index.vector;

import org.apache.hudi.avro.model.HoodieVectorIndexInfo;
import org.apache.hudi.common.model.HoodieFileGroupId;

import java.util.Collections;
import java.util.List;

public class DefaultVectorIndex implements VectorIndex {

  public DefaultVectorIndex(HoodieVectorIndexInfo vectorIndexInfo) {

  }

  @Override
  public boolean isInitialized() {
    return false;
  }

  @Override
  public void createIndex(Iterable<VectorIndexUpdate> initialData) {
    // No-op
  }

  @Override
  public void updateIndex(Iterable<VectorIndexUpdate> updates) {
    // No-op
  }

  @Override
  public String persist() {
    return null;
  }

  @Override
  public List<HoodieFileGroupId> getManagedFileGroups() {
    return Collections.emptyList();
  }
}
