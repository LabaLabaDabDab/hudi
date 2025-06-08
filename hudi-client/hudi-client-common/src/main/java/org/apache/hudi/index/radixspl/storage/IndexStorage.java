package org.apache.hudi.index.radixspl.storage;

import org.apache.hudi.index.radixspl.model.RadixSplineModel;
import java.util.Optional;

public interface IndexStorage {
    Optional<RadixSplineModel> loadIndex();
    void saveIndex(RadixSplineModel model);
    void deleteIndex();
    boolean indexExists();
}