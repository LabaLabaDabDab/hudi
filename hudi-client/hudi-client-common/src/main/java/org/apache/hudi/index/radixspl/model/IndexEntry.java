package org.apache.hudi.index.radixspl.model;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.ValidationUtils;

import java.io.*;
import java.util.Objects;


public final class IndexEntry implements Serializable, Comparable<IndexEntry> {
    private static final long serialVersionUID = 1L;

    private final HoodieKey key;
    private final String fileId;
    private final long filePosition;

    public IndexEntry(HoodieKey key, String fileId, long filePosition) {
        ValidationUtils.checkArgument(key != null, "Key must not be null");
        ValidationUtils.checkArgument(fileId != null && !fileId.isEmpty(), "FileId must not be empty");
        ValidationUtils.checkArgument(filePosition >= 0, "File position must be positive");

        this.key = key;
        this.fileId = fileId;
        this.filePosition = filePosition;
    }

    public HoodieKey getKey() {
        return key;
    }

    public String getFileId() {
        return fileId;
    }

    public long getFilePosition() {
        return filePosition;
    }

    @Override
    public int compareTo(IndexEntry other) {
        int cmp = key.getPartitionPath().compareTo(other.key.getPartitionPath());
        return (cmp != 0)
                ? cmp
                : key.getRecordKey().compareTo(other.key.getRecordKey());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexEntry)) return false;
        IndexEntry that = (IndexEntry) o;
        return filePosition == that.filePosition
                && key.equals(that.key)
                && fileId.equals(that.fileId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, fileId, filePosition);
    }

    @Override
    public String toString() {
        return "IndexEntry{" +
                "key=" + key +
                ", fileId='" + fileId + '\'' +
                ", filePosition=" + filePosition +
                '}';
    }

}