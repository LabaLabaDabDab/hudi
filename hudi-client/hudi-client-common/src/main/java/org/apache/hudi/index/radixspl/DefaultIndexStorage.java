package org.apache.hudi.index.radixspl;

import org.apache.hudi.common.util.HadoopConfigUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultIndexStorage implements IndexStorage {
    private final Path indexPath;
    private final Configuration hadoopConf;
    private static final Logger LOG = LoggerFactory.getLogger(DefaultIndexStorage.class);

    public DefaultIndexStorage(HoodieWriteConfig config) {
        this(config, config.getString(HoodieIndexConfig.RADIX_SPLINE_INDEX_PATH));
    }

    public DefaultIndexStorage(HoodieWriteConfig config, String indexFile) {
        Path path = new Path(indexFile);
        this.indexPath = path.isAbsolute() ? path : new Path(config.getBasePath(), indexFile);
        this.hadoopConf = HadoopConfigUtils.createHadoopConf(config.getProps());
        LOG.info("RadixSpline index file set to {}", this.indexPath);
    }

    private FileSystem getFileSystem() throws IOException {
        return indexPath.getFileSystem(hadoopConf);
    }

    @Override
    public Optional<RadixSplineModel> loadIndex() {
        try {
            FileSystem fs = getFileSystem();
            if (!fs.exists(indexPath)) {
                LOG.debug("Index file {} does not exist", indexPath);
                return Optional.empty();
            }
            try (FSDataInputStream in = fs.open(indexPath);
                 ObjectInputStream ois = new ObjectInputStream(in)) {
                RadixSplineModel model = (RadixSplineModel) ois.readObject();
                LOG.debug("Loaded RadixSplineModel from {}", indexPath);
                return Optional.of(model);
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new HoodieIndexException("Failed to load RadixSplineModel from " + indexPath, e);
        }
    }

    @Override
    public void saveIndex(RadixSplineModel model) {
        try {
            FileSystem fs = getFileSystem();
            Path parent = indexPath.getParent();
            if (!fs.exists(parent)) {
                fs.mkdirs(parent);
            }
            try (FSDataOutputStream out = fs.create(indexPath, true);
                 ObjectOutputStream oos = new ObjectOutputStream(out)) {
                oos.writeObject(model);
            }
            LOG.debug("Saved RadixSplineModel to {}", indexPath);
        } catch (IOException e) {
            throw new HoodieIndexException("Failed to save RadixSplineModel to " + indexPath, e);
        }
    }

    @Override
    public void deleteIndex() {
        try {
            FileSystem fs = getFileSystem();
            if (fs.exists(indexPath)) {
                fs.delete(indexPath, false);
            }
            LOG.debug("Deleted index file {}", indexPath);
        } catch (IOException e) {
            throw new HoodieIndexException("Failed to delete RadixSplineModel at " + indexPath, e);
        }
    }

    @Override
    public boolean indexExists() {
        try {
            FileSystem fs = getFileSystem();
            boolean exists = fs.exists(indexPath);
            LOG.debug("Index file {} exists -> {}", indexPath, exists);
            return exists;
        } catch (IOException e) {
            throw new HoodieIndexException("Failed to check existence of RadixSplineModel at " + indexPath, e);
        }
    }
}
