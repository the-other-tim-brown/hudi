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

package org.apache.hudi.metadata;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

// TODO rename
public class TmpFileWrapper {
  private final FileStatus fileStatus;
  private final String commitTime;
  private final String fileGroupId;

  public TmpFileWrapper(final FileStatus fileStatus, final String commitTime, final String fileGroupId) {
    this.fileStatus = fileStatus;
    this.commitTime = commitTime;
    this.fileGroupId = fileGroupId;
  }

  public boolean isDataFile() {
    return false;
  }

  public String getCommitTime() {
    return commitTime;
  }

  public String getFileGroupId() {
    return fileGroupId;
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public Path getPath() {
    return fileStatus.getPath();
  }

  public static TmpFileWrapper fromFileStatus(FileStatus fileStatus) {
    return null;
  }

  public static TmpFileWrapper[] fromFileStatusArray(FileStatus[] fileStatuses) {
    return null;
  }
}
