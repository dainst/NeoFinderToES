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
public class FileInfoCollector implements Callable<Integer> {

    private final BlockingQueue<ArchivedFileInfo> queue;
    
    private final ESService esService;
        
    private final boolean verbose;

    private final BulkIndexer bulkIndexer;
    
    private Thread myself;
    
    public FileInfoCollector(final File volume, final ESService esService, final BlockingQueue queue,
            final boolean verbose) {
    
        this.queue = queue;
        this.esService = esService;
        this.verbose = verbose;
        bulkIndexer = new BulkIndexer(esService, volume.toString(), verbose);
    }

    @Override
    public Integer call() {
        myself = Thread.currentThread();
        int filesSubmitted = 0;
        while (!myself.isInterrupted()) {
            try {
                List<ArchivedFileInfo> fileInfos = new ArrayList<>();
                synchronized (queue) {
                    queue.drainTo(fileInfos);
                }
                filesSubmitted += fileInfos.size();
                
                fileInfos.stream().forEach(fileInfo -> bulkIndexer.add(fileInfo));
            } catch (Exception e) {
                System.err.println("FileInfoCollector interrupted! " + e);
                myself.interrupt();
            }
        }
        return filesSubmitted;
    }
    
    public void interrupt(final int filesRead) {
        bulkIndexer.close(filesRead);
        myself.interrupt();
    }
}
