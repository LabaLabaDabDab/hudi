package org.apache.hudi.index.radixspl;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.ValidationUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RadixSplineModel implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RadixSplineModel.class);
    private final int radixBits;
    private final double splineMaxError;
    private final boolean globalIndex;

    private transient AtomicBoolean built = new AtomicBoolean(false);
    private final List<IndexEntry> buffer = new ArrayList<>();

    private SplinePoint[] splinePoints;
    private List<SplineLine> splineLines = new ArrayList<>();
    private int sortedKeysCount;
    public int[] radixTable;

    public RadixSplineModel(int radixBits, double splineMaxError, boolean globalIndex) {
        ValidationUtils.checkArgument(radixBits > 0, "radixBits must be > 0");
        ValidationUtils.checkArgument(splineMaxError >= 0, "splineMaxError must be >= 0");
        this.radixBits = radixBits;
        this.splineMaxError = splineMaxError;
        this.globalIndex = globalIndex;
        LOG.debug("Created RadixSplineModel radixBits={}, maxError={}, global={}", radixBits, splineMaxError, globalIndex);
    }

    public void addEntries(List<IndexEntry> entries) {
        LOG.debug("Adding {} entries to model", entries.size());
        buffer.addAll(entries);
        built.set(false);
    }

    public synchronized void build() {
        LOG.debug("Building RadixSpline model with {} buffered entries", buffer.size());
        if (buffer.isEmpty()) {
            this.splinePoints = new SplinePoint[0];
            this.radixTable = new int[(1 << radixBits) + 1];
            built.set(true);
            return;
        }
        buffer.sort(Comparator.comparingLong(e -> toNumeric(e.getKey())));
        int n = buffer.size();
        sortedKeysCount = n;
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
        LOG.debug("Model build complete");
        LOG.debug("Radix table: {}", Arrays.toString(radixTable));
        LOG.debug("Spline points: {}", Arrays.toString(splinePoints));
    }

    public Optional<IndexEntry> query(HoodieKey lookupKey) {
        LOG.debug("Querying key {}", lookupKey);
        if (!built.get()) {
            build();
        }
        if (buffer.isEmpty()) {
            return Optional.empty();
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
                    LOG.debug("Found entry for key {}", lookupKey);
                    return Optional.of(e);
                }
            }
        }
        LOG.debug("No entry found for key {}", lookupKey);
        return Optional.empty();
    }

    private @NotNull SearchInterval getSearchInterval(long k) {
        LOG.debug("Finding search interval for {}", k);
        if (splinePoints.length == 0) {
            return new SearchInterval(0, 0);
        }
        int prefix = extractRadix(k);
        int segIdx = radixTable[prefix];
        int lastIdx = splineLines.size() - 1;
        if (segIdx < 0) {
            segIdx = 0;
        } else if (segIdx > lastIdx) {
            segIdx = lastIdx;
        }
        double y = splineLines.get(segIdx).calculateY(k);
        int positionHat = (int) Math.round(y);
        positionHat = Math.max(0, Math.min(sortedKeysCount - 1, positionHat));
        int start = (int) Math.max(0, positionHat - splineMaxError);
        int end = (int) Math.min(sortedKeysCount - 1, positionHat + splineMaxError);
        return new SearchInterval(start, end);
    }

    private void buildSplinePoints(long[] keys, String[] fileIds, int[] offsets) {
        LOG.debug("Building spline points for {} keys", keys.length);
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
        splineLines = new ArrayList<>();
        for (int i = 0; i < splinePoints.length - 1; i++) {
            splineLines.add(new SplineLine(splinePoints[i], splinePoints[i + 1]));
        }
    }

    private void buildRadixTable() {
        LOG.debug("Building radix table with {} bits", radixBits);
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
        LOG.debug("Radix table build complete");
    }

    private long toNumeric(HoodieKey key) {
        String s;
        if (globalIndex) {
            s = key.getRecordKey();
        } else {
            s = key.getPartitionPath() + '|' + key.getRecordKey();
        }
        long numeric = (long) (s.hashCode()) & 0xFFFFFFFFL;
        LOG.debug("Converted key {} to numeric {}", key, numeric);
        return numeric;
    }

    private int extractRadix(long numericKey) {
        int r = (int) (numericKey >>> (Integer.SIZE - radixBits));
        LOG.debug("Extracted radix {} from numeric {}", r, numericKey);
        return r;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        LOG.debug("Reading RadixSplineModel from stream");
        in.defaultReadObject();
        built = new AtomicBoolean(false);
        splineLines = new ArrayList<>();
        LOG.debug("Deserialized RadixSplineModel; marked as not built");
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
        LOG.debug("Clearing RadixSpline model");
        buffer.clear();
        splineLines.clear();
        splinePoints = null;
        radixTable = null;
        sortedKeysCount = 0;
        built.set(false);
    }

    public List<IndexEntry> getEntries() {
        return new ArrayList<>(buffer);
    }

    public int[] getRadixTable() {
        return radixTable == null ? new int[0] : Arrays.copyOf(radixTable, radixTable.length);
    }

    public SplinePoint[] getSplinePoints() {
        return splinePoints == null ? new SplinePoint[0] : Arrays.copyOf(splinePoints, splinePoints.length);
    }

    public boolean isBuilt() {
        boolean b = built.get();
        LOG.debug("isBuilt -> {}", b);
        return b;
    }
}
