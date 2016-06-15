package org.dainst.arachne;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class to fetch file information added by <code>DirectoryCrawlers</code> from the queue and add it to the
 * elasticsearch index.
 *
 * @author Reimar Grabowski
 */
public class Indexer implements Callable<Long> {

    private final BlockingQueue<ArchivedFileInfo> queue;
    private volatile boolean isRunning = true;
    private Thread myself;
    private final String volume;
    private String hostname;
    
    private final ESService esService;
    private final String indexName;
    
    private final boolean verbose;

    private long indexedFiles = 0;
    
    public Indexer(final File volume, final String indexName, final ESService esService, final BlockingQueue queue,
            final boolean verbose) {
        this.queue = queue;
        this.volume = volume.toString();
        try {
            this.hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            this.hostname = "unknown";
        }
        
        this.esService = esService;
        this.indexName = indexName;
        this.verbose = verbose;
    }

    @Override
    public Long call() {
        myself = Thread.currentThread();
        while (isRunning) {
            try {
                index(queue.take());
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        }
        return indexedFiles;
    }

    public void index(final ArchivedFileInfo fileInfo) {
        fileInfo.setVolume(volume);
        fileInfo.setCatalog(hostname);
        fileInfo.setIndex(indexName);
        
        final ObjectMapper mapper = new ObjectMapper();

        try {
            byte[] jsonAsBytes = mapper.writeValueAsBytes(fileInfo);
            if (verbose) {
                System.out.println(mapper.writeValueAsString(fileInfo));
            }
            
            String id = esService.addToIndex(indexName, jsonAsBytes);
            if (id == null || id.isEmpty()) {
                Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, 
                        "Failed to add entry {0}", mapper.writeValueAsString(fileInfo));
            }
        } catch (JsonProcessingException ex) {
            Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
        }
        indexedFiles++;
    }

    public void terminate() {
        isRunning = false;
        myself.interrupt();
    }
}
