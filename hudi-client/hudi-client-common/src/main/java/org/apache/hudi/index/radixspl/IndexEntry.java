package org.apache.hudi.index.radixspl;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.ValidationUtils;

import java.io.*;
import java.util.Objects;

public final class IndexEntry implements Serializable, Comparable<IndexEntry> {
    private static final long serialVersionUID = 1L;
    private static final byte MAGIC = (byte) 0xB4;
    private static final byte VERSION = 1;

    private final HoodieKey key;
    private final String instantTime;
    private final String fileId;
    private final long filePosition;

    public IndexEntry(HoodieKey key, String instantTime, String fileId, long filePosition) {
        ValidationUtils.checkArgument(key != null, "Key must not be null");
        ValidationUtils.checkArgument(instantTime != null && !instantTime.isEmpty(), "Instant time must not be empty");
        ValidationUtils.checkArgument(fileId != null && !fileId.isEmpty(), "FileId must not be empty");
        ValidationUtils.checkArgument(filePosition >= 0, "File position must be positive");

        this.key = key;
        this.instantTime = instantTime;
        this.fileId = fileId;
        this.filePosition = filePosition;
    }


    public byte[] toBytes() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeByte(MAGIC);
            dos.writeByte(VERSION);
            dos.writeUTF(key.getRecordKey());
            dos.writeUTF(key.getPartitionPath());
            dos.writeUTF(instantTime);
            dos.writeUTF(fileId);
            dos.writeLong(filePosition);
            dos.flush();
            return bos.toByteArray();
        }
    }

    public static IndexEntry fromBytes(byte[] bytes) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             DataInputStream dis = new DataInputStream(bis)) {
            byte magic = dis.readByte();
            if (magic != MAGIC) {
                throw new IOException("Invalid magic header: " + magic);
            }
            byte version = dis.readByte();
            if (version != VERSION) {
                throw new IOException("Unsupported version: " + version);
            }
            String recordKey = dis.readUTF();
            String partitionPath = dis.readUTF();
            String instantTime = dis.readUTF();
            String fileId = dis.readUTF();
            long filePosition = dis.readLong();
            return new IndexEntry(new HoodieKey(recordKey, partitionPath), instantTime, fileId, filePosition);
        }
    }

    public HoodieKey getKey() {
        return key;
    }

    public String getFileId() {
        return fileId;
    }

    public String getInstantTime() {
        return instantTime;
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
                && fileId.equals(that.fileId)
                && instantTime.equals(that.instantTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, instantTime, fileId, filePosition);
    }

    @Override
    public String toString() {
        return "IndexEntry{" +
                "key=" + key +
                ", instantTime='" + instantTime + '\'' +
                ", fileId='" + fileId + '\'' +
                ", filePosition=" + filePosition +
                '}';
    }
}