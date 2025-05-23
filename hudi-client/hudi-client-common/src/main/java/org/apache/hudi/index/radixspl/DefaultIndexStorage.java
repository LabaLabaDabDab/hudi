package org.apache.hudi.index.radixspl;

import org.apache.hudi.common.util.HadoopConfigUtils;
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


public class DefaultIndexStorage implements IndexStorage {
    private final Path indexPath;
    private final Configuration hadoopConf;

    public DefaultIndexStorage(HoodieWriteConfig config) {
        this.indexPath = new Path(config.getBasePath(), ".hoodie/radix-spline-index.idx");
        this.hadoopConf = HadoopConfigUtils.createHadoopConf(config.getProps());
    }

    private FileSystem getFileSystem() throws IOException {
        return indexPath.getFileSystem(hadoopConf);
    }

    @Override
    public Optional<RadixSplineModel> loadIndex() {
        try {
            FileSystem fs = getFileSystem();
            if (!fs.exists(indexPath)) {
                return Optional.empty();
            }
            try (FSDataInputStream in = fs.open(indexPath);
                 ObjectInputStream ois = new ObjectInputStream(in)) {
                return Optional.of((RadixSplineModel) ois.readObject());
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
        } catch (IOException e) {
            throw new HoodieIndexException("Failed to delete RadixSplineModel at " + indexPath, e);
        }
    }

    @Override
    public boolean indexExists() {
        try {
            FileSystem fs = getFileSystem();
            return fs.exists(indexPath);
        } catch (IOException e) {
            throw new HoodieIndexException("Failed to check existence of RadixSplineModel at " + indexPath, e);
        }
    }
}
