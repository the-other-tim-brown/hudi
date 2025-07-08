package org.apache.hudi.common.table.read;

/**
 * Simple object to represent the result of a merge operation.
 */
class MergeResult<T> {
  private final boolean isDelete;
  private final T record;

  public MergeResult(boolean isDelete, T record) {
    this.isDelete = isDelete;
    this.record = record;
  }

  public boolean isDelete() {
    return isDelete;
  }

  public T getRecord() {
    return record;
  }
}
