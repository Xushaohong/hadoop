package org.apache.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;

/**
 * create:lichunxiao
 * Date:2019-06-05
 */
public class Mdfs extends Hdfs {
    /**
     * This constructor has the signature needed by
     * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}
     *
     * @param theUri which must be that of Hdfs
     * @param conf   configuration
     * @throws IOException
     */
    Mdfs(URI theUri, Configuration conf) throws IOException, URISyntaxException {
        super(theUri, conf);
    }

    @Override
    public String getUriPath(final Path p) {
        checkPath(p);
        String s = p.toUri().toString();
        return s;
    }
}
