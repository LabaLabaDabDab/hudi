package org.apache.hudi.index.radixspl;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.radixspl.model.IndexEntry;
import org.apache.hudi.index.radixspl.model.RadixSplineModel;
import org.apache.hudi.index.radixspl.storage.DefaultIndexStorage;
import org.apache.hudi.index.radixspl.storage.IndexStorage;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.client.WriteStatus;

import java.util.Collections;
import java.util.List;
import java.util.Optional;


public class HoodieRadixSplineIndex<T extends HoodieRecordPayload>
        extends HoodieIndex<T, HoodieRecord<T>> {

    private final RadixSplineModel model;
    private final IndexStorage storage;
    private final boolean isGlobal;

    public HoodieRadixSplineIndex(HoodieWriteConfig config, HoodieEngineContext context) {
        super(config);
        this.storage = new DefaultIndexStorage(config);
        this.isGlobal   = config.getBoolean(HoodieIndexConfig.GLOBAL_INDEX_ENABLED);
        int radixBits   = config.getInt(    HoodieIndexConfig.RADIX_SPLINE_INDEX_RADIX_BITS);
        double maxError = config.getDouble( HoodieIndexConfig.RADIX_SPLINE_INDEX_MAX_ERROR);
        this.model = storage.loadIndex()
                .orElseGet(() -> new RadixSplineModel(radixBits, maxError, isGlobal));
    }

    @Override
    public <R> HoodieData<HoodieRecord<R>> tagLocation(
            HoodieData<HoodieRecord<R>> records,
            HoodieEngineContext context,
            HoodieTable table) throws HoodieIndexException {

        if (!model.isBuilt()) {
            List<IndexEntry> existing = fetchCurrentIndexEntries(table, context);
            model.addEntries(existing);
            model.build();
            storage.saveIndex(model);
        }

        return records.map(record -> {
            Optional<IndexEntry> entry = model.query(record.getKey());
            entry.ifPresent(e -> record.setCurrentLocation(
                    new HoodieRecordLocation(e.getFileId(), null)
            ));
            return record;
        });
    }

    @Override
    public HoodieData<WriteStatus> updateLocation(
            HoodieData<WriteStatus> writeStatuses,
            HoodieEngineContext context,
            HoodieTable table) throws HoodieIndexException {

        List<IndexEntry> allEntries = fetchCurrentIndexEntries(table, context);
        model.clear();
        model.addEntries(allEntries);
        model.build();

        storage.saveIndex(model);

        return writeStatuses;
    }

    @Override
    public boolean rollbackCommit(String instantTime) {
        storage.deleteIndex();
        return true;
    }

    @Override
    public boolean isGlobal() {
        return isGlobal;
    }

    @Override
    public boolean canIndexLogFiles() {
        return false;
    }

    @Override
    public boolean isImplicitWithStorage() {
        return false;
    }

    private List<IndexEntry> fetchCurrentIndexEntries(
            HoodieTable table, HoodieEngineContext context) {
        return storage.loadIndex()
                .map(RadixSplineModel::getEntries)
                .orElseGet(Collections::emptyList);
    }
}
