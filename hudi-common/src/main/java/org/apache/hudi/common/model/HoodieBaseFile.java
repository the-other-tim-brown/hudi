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

package org.apache.hudi.common.model;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.CachingPath;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static org.apache.hudi.hadoop.CachingPath.createRelativePathUnsafe;

/**
 * Hoodie base file - Represents metadata about Hudi file in DFS.
 * Supports APIs to get Hudi FileId, Commit Time and bootstrap file (if any).
 */
public class HoodieBaseFile extends BaseFile {

  private final String fileId;
  private final String commitTime;
  private Option<BaseFile> bootstrapBaseFile;

  public HoodieBaseFile(HoodieBaseFile dataFile) {
    super(dataFile);
    this.bootstrapBaseFile = dataFile.bootstrapBaseFile;
    this.fileId = dataFile.getFileId();
    this.commitTime = dataFile.getCommitTime();
  }

  public HoodieBaseFile(FileStatus fileStatus) {
    this(fileStatus, null);
  }

  public HoodieBaseFile(FileStatus fileStatus, BaseFile bootstrapBaseFile) {
    super(manipulateFileStatus(fileStatus));
    this.bootstrapBaseFile = Option.ofNullable(bootstrapBaseFile);
    this.fileId = FSUtils.getFileId(getFileName());
    this.commitTime = FSUtils.getCommitTime(getFileName());
  }

  public HoodieBaseFile(String filePath) {
    this(filePath, null);
  }

  public HoodieBaseFile(String filePath, BaseFile bootstrapBaseFile) {
    super(filePath);
    this.bootstrapBaseFile = Option.ofNullable(bootstrapBaseFile);
    this.fileId = FSUtils.getFileId(getFileName());
    this.commitTime = FSUtils.getCommitTime(getFileName());
  }

  private static FileStatus manipulateFileStatus(FileStatus fileStatus) {
    if (fileStatus == null) {
      return null;
    }
    if (!FSUtils.fileNameIsNewEncoding(fileStatus.getPath().getName())) {
      return fileStatus;
    }
    Path parent = fileStatus.getPath().getParent();
    String fileName = fileStatus.getPath().getName();
    String updatedFileName = FSUtils.fileNameOriginalPath(fileName);
    return new FileStatus(fileStatus.getLen(), fileStatus.isDirectory(), fileStatus.getReplication(),
        fileStatus.getBlockSize(), fileStatus.getModificationTime(), fileStatus.getAccessTime(),
        fileStatus.getPermission(), fileStatus.getOwner(), fileStatus.getGroup(),
        new CachingPath(parent, createRelativePathUnsafe(updatedFileName)));
  }

  public String getFileId() {
    return fileId;
  }

  public String getCommitTime() {
    return commitTime;
  }

  public Option<BaseFile> getBootstrapBaseFile() {
    return bootstrapBaseFile;
  }

  public void setBootstrapBaseFile(BaseFile bootstrapBaseFile) {
    this.bootstrapBaseFile = Option.ofNullable(bootstrapBaseFile);
  }

  @Override
  public String toString() {
    return "HoodieBaseFile{fullPath=" + getPath() + ", fileLen=" + getFileLen()
        + ", BootstrapBaseFile=" + bootstrapBaseFile.orElse(null) + '}';
  }
}
