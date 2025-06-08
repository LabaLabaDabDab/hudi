package org.apache.hudi.index.radixspl.model;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.ValidationUtils;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


public class RadixSplineModel implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int radixBits;
    private final double splineMaxError;
    private final boolean globalIndex;

    private transient AtomicBoolean built = new AtomicBoolean(false);
    private final List<IndexEntry> buffer = new ArrayList<>();

    private SplinePoint[] splinePoints;
    public int[] radixTable;

    public RadixSplineModel(int radixBits, double splineMaxError, boolean globalIndex) {
        ValidationUtils.checkArgument(radixBits > 0, "radixBits must be > 0");
        ValidationUtils.checkArgument(splineMaxError >= 0, "splineMaxError must be >= 0");
        this.radixBits = radixBits;
        this.splineMaxError = splineMaxError;
        this.globalIndex = globalIndex;
    }

    public void addEntries(List<IndexEntry> entries) {
        buffer.addAll(entries);
        built.set(false);
    }

    public synchronized void build() {
        if (buffer.isEmpty()) {
            throw new IllegalStateException("No entries to build");
        }
        buffer.sort(Comparator.comparingLong(e -> toNumeric(e.getKey())));
        int n = buffer.size();
        long[] keys = new long[n];
        String[] fileIds = new String[n];
        int[] offsets = new int[n];
        for (int i = 0; i < n; i++) {
            IndexEntry e = buffer.get(i);
            keys[i] = toNumeric(e.getKey());
            fileIds[i] = e.getFileId();
            offsets[i] = i;
        }
        buildSplinePoints(keys, fileIds, offsets);
        buildRadixTable();
        built.set(true);
    }

    public Optional<IndexEntry> query(HoodieKey lookupKey) {
        if (!built.get()) {
            build();
        }
        long k = toNumeric(lookupKey);
        SearchInterval interval = getSearchInterval(k);
        for (int i = interval.start; i <= interval.end; i++) {
            IndexEntry e = buffer.get(i);
            if (globalIndex) {
                if (e.getKey().getRecordKey().equals(lookupKey.getRecordKey())) {
                    return Optional.of(e);
                }
            } else {
                if (e.getKey().equals(lookupKey)) {
                    return Optional.of(e);
                }
            }
        }

        return Optional.empty();
    }

    private @NotNull SearchInterval getSearchInterval(long k) {
        int bucket = (int) (k >>> (Long.SIZE - radixBits));
        int lo = radixTable[bucket];
        int hi = radixTable[bucket + 1];
        int idx = Arrays.binarySearch(splinePoints, lo, hi, new SplinePoint(k, 0, extractRadix(k), null));
        if (idx < 0) idx = -idx - 2;
        idx = Math.max(0, idx);
        SearchInterval interval = new SearchInterval(
                splinePoints[idx].getOffset(),
                idx + 1 < splinePoints.length ? splinePoints[idx + 1].getOffset() : buffer.size() - 1
        );
        return interval;
    }

    private void buildSplinePoints(long[] keys, String[] fileIds, int[] offsets) {
        List<SplinePoint> pts = new ArrayList<>();
        pts.add(new SplinePoint(keys[0], offsets[0], extractRadix(keys[0]), fileIds[0]));
        int last = 0;
        for (int i = 1; i < keys.length; i++) {
            SplinePoint cand = new SplinePoint(keys[i], offsets[i], extractRadix(keys[i]), fileIds[i]);
            SplineLine line = new SplineLine(pts.get(last), cand);
            boolean ok = true;
            for (int j = last + 1; j < i; j++) {
                double pred = line.calculateY(keys[j]);
                if (Math.abs(pred - offsets[j]) > splineMaxError) {
                    ok = false;
                    break;
                }
            }
            if (!ok) {
                SplinePoint prev = new SplinePoint(
                        keys[i - 1], offsets[i - 1], extractRadix(keys[i - 1]), fileIds[i - 1]
                );
                pts.add(prev);
                last = pts.size() - 1;
            }
        }
        pts.add(new SplinePoint(
                keys[keys.length - 1], offsets[offsets.length - 1],
                extractRadix(keys[keys.length - 1]), fileIds[keys.length - 1]
        ));
        splinePoints = pts.toArray(new SplinePoint[0]);
    }

    private void buildRadixTable() {
        int size = (1 << radixBits) + 1;
        radixTable = new int[size];
        int cur = 0;
        for (int i = 0; i < splinePoints.length; i++) {
            int r = splinePoints[i].getRadix();
            while (cur <= r) {
                radixTable[cur++] = i;
            }
        }
        while (cur < size) {
            radixTable[cur++] = splinePoints.length - 1;
        }
    }

    private long toNumeric(HoodieKey key) {
        String s;
        if (globalIndex) {
            s = key.getRecordKey();
        } else {
            s = key.getPartitionPath() + '|' + key.getRecordKey();
        }
        return (long)(s.hashCode()) & 0xFFFFFFFFL;
    }


    private int extractRadix(long numericKey) {
        return (int) (numericKey >>> (Long.SIZE - radixBits));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        built = new AtomicBoolean(false);
    }

    public static class SearchInterval {
        public final int start;
        public final int end;
        public SearchInterval(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }

    public void clear() {
        buffer.clear();
        built.set(false);
    }

    public List<IndexEntry> getEntries() {
        return new ArrayList<>(buffer);
    }

    public boolean isBuilt() {
        return built.get();
    }
}
