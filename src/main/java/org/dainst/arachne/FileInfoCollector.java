package org.dainst.arachne;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * Class to fetch file information added by <code>DirectoryCrawlers</code> from the queue and add it to the
 * elasticsearch index.
 *
 * @author Reimar Grabowski
 */
public class FileInfoCollector implements Callable<List<ArchivedFileInfo>> {

    private final BlockingQueue<ArchivedFileInfo> queue;
    
    private final ESService esService;
        
    private final boolean verbose;

    private final List<ArchivedFileInfo> indexedFiles = new ArrayList<>();
    
    private Thread myself;
    
    public FileInfoCollector(final File volume, final ESService esService, final BlockingQueue queue,
            final boolean verbose) {
    
        this.queue = queue;
        this.esService = esService;
        this.verbose = verbose;
    }

    @Override
    public List<ArchivedFileInfo> call() {
        myself = Thread.currentThread();
        while (!myself.isInterrupted()) {
            try {
                indexedFiles.add(queue.take());
            } catch (Exception e) {
                myself.interrupt();
            }
        }
        return indexedFiles;
    }
    
    public void interrupt() {
        myself.interrupt();
    }
}
