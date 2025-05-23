package org.apache.hudi.index.radixspl;

import java.util.Optional;

public interface IndexStorage {
    Optional<RadixSplineModel> loadIndex();
    void saveIndex(RadixSplineModel model);
    void deleteIndex();
    boolean indexExists();
}
