package org.dainst.arachne;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
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

    private final File volume;
    
    private String hostname;
    
    private Thread myself;

    public FileInfoCollector(final File volume, final ESService esService, final BlockingQueue queue,
            final boolean verbose) {

        this.queue = queue;
        this.esService = esService;
        this.verbose = verbose;
        this.volume = volume;
        
        bulkIndexer = new BulkIndexer(esService, verbose);
        try {
            this.hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            this.hostname = "unknown";
        }
    }

    @Override
    public Integer call() {
        myself = Thread.currentThread();
        int filesSubmitted = 0;
        while (!myself.isInterrupted()) {
            try {
                if (queue.size() > 0) {
                    List<ArchivedFileInfo> fileInfos = new ArrayList<>();
                    synchronized (queue) {
                        queue.drainTo(fileInfos);
                    }
                    filesSubmitted += fileInfos.size();

                    fileInfos.stream().forEach(fileInfo -> {
                        fileInfo.setVolume(volume.toString());
                        fileInfo.setCatalog(hostname);
                        bulkIndexer.add(fileInfo);
                    });
                    fileInfos = null;
                } else {
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                if (!(e instanceof InterruptedException)) { 
                    System.err.println("FileInfoCollector interrupted! " + e);
                }
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
