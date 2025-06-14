package org.apache.hudi.index.radixspl;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.*;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.HoodieKeyLocationFetchHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


public class HoodieRadixSplineIndex<T extends HoodieRecordPayload>
        extends HoodieIndex<T, HoodieRecord<T>> {

    private final RadixSplineModel model;
    private final IndexStorage storage;
    private final boolean isGlobal;
    private static final Logger LOG = LoggerFactory.getLogger(HoodieRadixSplineIndex.class);

    public HoodieRadixSplineIndex(HoodieWriteConfig config) {
        super(config);
        String indexFile = config.getString(HoodieIndexConfig.RADIX_SPLINE_INDEX_PATH);
        this.storage = new DefaultIndexStorage(config, indexFile);
        this.isGlobal   = config.getBoolean(HoodieIndexConfig.GLOBAL_INDEX_ENABLED);
        int radixBits   = config.getInt(    HoodieIndexConfig.RADIX_SPLINE_INDEX_RADIX_BITS);
        double maxError = config.getDouble( HoodieIndexConfig.RADIX_SPLINE_INDEX_MAX_ERROR);
        LOG.info("Initializing RadixSplineIndex: file={}, radixBits={}, maxError={}, global={}",
                indexFile, radixBits, maxError, isGlobal);
        this.model = storage.loadIndex()
                .map(model -> {
                    LOG.debug("Loaded existing RadixSpline index model");
                    return model;
                })
                .orElseGet(() -> {
                    LOG.debug("No existing index found, creating new model");
                    return new RadixSplineModel(radixBits, maxError, isGlobal);
                });
    }

    @Override
    public <R> HoodieData<HoodieRecord<R>> tagLocation(
            HoodieData<HoodieRecord<R>> records,
            HoodieEngineContext context,
            HoodieTable table) throws HoodieIndexException {

        LOG.debug("tagLocation called with {} records", records.count());
        LOG.debug("Tagging record locations using RadixSpline index");
        List<IndexEntry> existing = fetchCurrentIndexEntries(table, context);
        if (!model.isBuilt() && storage.indexExists()) {
            if (!existing.isEmpty()) {
                model.addEntries(existing);
                model.build();
                storage.saveIndex(model);
                LOG.debug("Loaded and built index from storage");
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Radix table: {}", Arrays.toString(model.getRadixTable()));
                    LOG.debug("Spline points: {}", Arrays.toString(model.getSplinePoints()));
                }
            }
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

        LOG.debug("updateLocation called with {} write statuses", writeStatuses.count());
        LOG.debug("Updating index with new write statuses");

        List<IndexEntry> allEntries = fetchCurrentIndexEntries(table, context);
        List<WriteStatus> statuses = writeStatuses.collectAsList();
        for (WriteStatus status : statuses) {
            for (HoodieRecordDelegate delegate : status.getWrittenRecordDelegates()) {
                if (delegate.getIgnoreIndexUpdate()) {
                    continue;
                }
                Option<HoodieRecordLocation> loc = delegate.getNewLocation();
                if (loc.isPresent() && HoodieRecordLocation.isPositionValid(loc.get().getPosition())) {
                    HoodieRecordLocation l = loc.get();
                    allEntries.add(new IndexEntry(
                            delegate.getHoodieKey(),
                            l.getInstantTime(),
                            l.getFileId(),
                            l.getPosition()
                    ));
                }
            }
        }
        model.clear();
        model.addEntries(allEntries);
        model.build();

        storage.saveIndex(model);
        LOG.debug("Index updated and persisted");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Radix table: {}", Arrays.toString(model.getRadixTable()));
            LOG.debug("Spline points: {}", Arrays.toString(model.getSplinePoints()));
        }


        return writeStatuses;
    }

    @Override
    public boolean rollbackCommit(String instantTime) {
        LOG.info("rollbackCommit called for {}", instantTime);
        LOG.info("Rolling back commit {} and deleting index", instantTime);
        storage.deleteIndex();
        return true;
    }

    @Override
    public boolean isGlobal() {
        LOG.debug("isGlobal called -> {}", isGlobal);
        return isGlobal;
    }

    @Override
    public boolean canIndexLogFiles() {
        LOG.debug("canIndexLogFiles called -> false");
        return false;
    }

    @Override
    public boolean isImplicitWithStorage() {
        LOG.debug("isImplicitWithStorage called -> false");
        return false;
    }

    private List<IndexEntry> fetchCurrentIndexEntries(
            HoodieTable table, HoodieEngineContext context) {

        LOG.debug("fetchCurrentIndexEntries called");
        LOG.debug("Fetching current index entries");
        List<String> partitions = table.isPartitioned()
                ? FSUtils.getAllPartitionPaths(context, table.getMetaClient().getStorage(),
                config.getMetadataConfig(), table.getMetaClient().getBasePath())
                : Collections.singletonList(HoodieTableMetadata.EMPTY_PARTITION_NAME);

        List<Pair<String, HoodieBaseFile>> baseFiles =
                HoodieIndexUtils.getLatestBaseFilesForAllPartitions(partitions, context, table);

        int parallelism = Math.max(1, baseFiles.size());

        HoodieData<IndexEntry> indexEntries = context.parallelize(baseFiles, parallelism)
                .flatMap(partitionBaseFile -> new HoodieKeyLocationFetchHandle(
                        config, table, partitionBaseFile, Option.empty()).locations())
                .map(obj -> {
                    Pair<HoodieKey, HoodieRecordLocation> entry = (Pair<HoodieKey, HoodieRecordLocation>) obj;
                    return new IndexEntry(entry.getLeft(),
                            entry.getRight().getInstantTime(),
                            entry.getRight().getFileId(), entry.getRight().getPosition());
                });

        List<IndexEntry> entries = indexEntries.collectAsList();
        LOG.debug("Fetched {} index entries", entries.size());
        return entries;
    }
}