package org.dainst.arachne;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 *
 * @author Reimar Grabowski
 */
public class BulkIndexer {

    private ESService esService;

    private final BulkProcessorListener listener = new BulkProcessorListener();

    private final BulkProcessor bulkProcessor;

    final ObjectMapper mapper = new ObjectMapper();
    
    private int filesIndexed = 0;
    
    private int filesSubmitted = 0;
    
    private int filesRead = 0;
    
    private boolean verbose;
    
    public BulkIndexer(final ESService esService, final boolean verbose) {
        this.esService = esService;
        this.verbose = verbose;

        bulkProcessor = BulkProcessor.builder(esService.getClient(), listener)
                .setBulkActions(100000)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .build();
    }

    void add(final ArchivedFileInfo fileInfo) {
        fileInfo.setIndex(esService.getIndexName());

        try {
            final byte[] jsonAsBytes = mapper.writeValueAsBytes(fileInfo);
            bulkProcessor.add(esService.getClient().prepareIndex(esService.getIndexName(), "entity", fileInfo.getPath())
                    .setSource(jsonAsBytes).request());
            filesSubmitted++;
        } catch (JsonProcessingException ex) {
            System.err.println("Could not map file info to JSON. Cause: " + ex);
        }
    }

    void close(final int filesRead) {
        this.filesRead = filesRead;
        while (filesRead > filesSubmitted) {            
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        bulkProcessor.close();
        // wait a little bit to let the bulk request finish as bulkprocessoor.close() is non-blocking
        int retries = 0;
        while (listener.getOpenRequests() > 0 && !listener.hasFailed() && retries < 60) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            retries++;
        }
        if (retries > 59) {
            throw new RuntimeException("Bulk request did not finish in 1 minute.");
        }
    }

    class BulkProcessorListener implements BulkProcessor.Listener {

        // used to check if last bulk request has finished
        private int openRequests = 0;
        private boolean error = false;

        public int getOpenRequests() {
            return openRequests;
        }

        public boolean hasFailed() {
            return error;
        }

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            openRequests++;
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            filesIndexed += request.numberOfActions();
            if (filesRead > 0) {
                System.out.print("\rImported file information: " + filesIndexed + "/" + filesRead);
            } else {
                System.out.print("\rImported file information: " + filesIndexed + "/?");
            }
            if (verbose) {
                System.out.println(" [executionId " + executionId + ": " + request.numberOfActions() + " documents]");
            } else { 
                System.out.println("");
            }
            openRequests--;
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            System.err.println(String.format("Error executing bulk id: %s", executionId) + failure);
            openRequests--;
            error = true;
        }
    }
}
