package org.apache.hudi.index.radixspl;

import org.apache.hudi.common.util.ValidationUtils;

import java.io.*;
import java.util.Objects;

public final class SplinePoint implements Serializable, Comparable<SplinePoint> {
    private static final byte MAGIC = (byte) 0xB3;
    private static final byte VERSION = 1;

    private final long key;
    private final int offset;
    private final int radix;
    private final String fileId;

    public SplinePoint(long key, int offset, int radix, String fileId) {
        ValidationUtils.checkArgument(offset >= 0, "offset must be >= 0");
        ValidationUtils.checkArgument(radix >= 0, "radix must be >= 0");
        if (fileId != null) {
            ValidationUtils.checkArgument(!fileId.isEmpty(), "fileId must not be empty");
        }
        this.key = key;
        this.offset = offset;
        this.radix = radix;
        this.fileId = fileId;
    }

    public long getKey() { return key; }
    public int getOffset() { return offset; }
    public int getRadix() { return radix; }
    public String getFileId() { return fileId; }

    @Override
    public int compareTo(SplinePoint other) {
        return Long.compare(this.key, other.key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SplinePoint)) return false;
        SplinePoint that = (SplinePoint) o;
        return key == that.key && offset == that.offset &&
                radix == that.radix && fileId.equals(that.fileId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, offset, radix, fileId);
    }

    @Override
    public String toString() {
        return String.format("SplinePoint{key=%d, offset=%d, radix=%d, fileId='%s'}",
                key, offset, radix, fileId);
    }

    public byte[] toBytes() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeByte(MAGIC);
            dos.writeByte(VERSION);
            dos.writeLong(key);
            dos.writeInt(offset);
            dos.writeInt(radix);
            dos.writeUTF(fileId);
            dos.flush();
            return bos.toByteArray();
        }
    }

    public static SplinePoint fromBytes(byte[] bytes) throws IOException {
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
            long key = dis.readLong();
            int offset = dis.readInt();
            int radix = dis.readInt();
            String fileId = dis.readUTF();
            return new SplinePoint(key, offset, radix, fileId);
        }
    }
}
