package org.dainst.arachne;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.LinkOption;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Reimar Grabowski
 */
public class FileSystemScanner {

    private final ESService esService;

    private String volume;

    private boolean verbose;

    private String hostname;
    private boolean strict;

    public FileSystemScanner(final ESService esService) {
        this.esService = esService;

        try {
            this.hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            this.hostname = "unknown";
        }
    }

    public void scan(final File scanDirectory, final int maxThreads, final int mimeInfo, final boolean strict
            , final boolean verbose) throws IOException {

        this.volume = scanDirectory.toString();
        this.verbose = verbose;
        this.strict = strict;
        Indexer indexer = null;
        try {
            System.out.format("\rScanning %s...\n", scanDirectory);

            BlockingQueue<ArchivedFileInfo> queue = new LinkedBlockingQueue<>();

            indexer = new Indexer(scanDirectory, esService, queue, verbose);
            ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
            Future<List<ArchivedFileInfo>> indexedFiles = (Future<List<ArchivedFileInfo>>) singleThreadExecutor
                    .submit(indexer);

            DirectoryCrawler crawler;

            crawler = new DirectoryCrawler(scanDirectory.toPath().toRealPath(LinkOption.NOFOLLOW_LINKS), mimeInfo
                    , strict, queue);
            ForkJoinPool pool = new ForkJoinPool(maxThreads);

            long startTime = new Date().getTime();

            // clean up if the execution is finished or terminated (for example by ctrl+c)
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    singleThreadExecutor.shutdownNow();
                    pool.shutdownNow();
                    esService.close();
                }
            });

            boolean errors = false;
            try {
                errors = pool.invoke(crawler);
            } catch (CancellationException e) {
                System.err.println("Thread pool invokation cancelled. Cause: " + e);
            }

            while (!queue.isEmpty()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                    System.err.println("Failure while waiting for queue to get depleted. Cause: " + ex);
                }
            }

            indexer.terminate();

            List<ArchivedFileInfo> fileInfos = null;
            try {
                fileInfos = indexedFiles.get();
            } catch (InterruptedException | ExecutionException ex) {
                System.err.println("Failure while retrieving file/directoy information. Cause: " + ex);
            }

            if (fileInfos != null && !fileInfos.isEmpty()) {
                System.out.println("\rFiles read: " + fileInfos.size() + "\n");

                if (!strict || (strict && !errors)) {
                    System.out.println("Importing into elasticsearch index...");
                    for (ArchivedFileInfo fileInfo : fileInfos) {
                        index(fileInfo);
                    }
                    System.out.println("\rIndexed files: " + fileInfos.size());
                } else {
                    System.out.println("\rNo data imported.");
                }
                if (verbose) {
                    long diff = new Date().getTime() - startTime;
                    String timeTaken = String.format("%02d min, %02d sec", TimeUnit.MILLISECONDS.toMinutes(diff)
                            , TimeUnit.MILLISECONDS.toSeconds(diff) 
                            - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(diff)));
                    System.out.println("\rElapsed time: " + timeTaken);
                }
            }
            singleThreadExecutor.shutdownNow();
        } catch (IOException ex) {
            if (indexer != null) {
                indexer.terminate();
            }
            throw ex;
        }
    }

    private void index(final ArchivedFileInfo fileInfo) {
        fileInfo.setVolume(volume);
        fileInfo.setCatalog(hostname);
        fileInfo.setIndex(esService.getIndexName());

        final ObjectMapper mapper = new ObjectMapper();

        try {
            byte[] jsonAsBytes = mapper.writeValueAsBytes(fileInfo);
            if (verbose) {
                System.out.println(mapper.writeValueAsString(fileInfo));
            }

            String id = esService.addToIndex(jsonAsBytes, fileInfo.getPath());
            if (id == null || id.isEmpty()) {
                System.err.println("Failed to add entry: \n" + mapper.writeValueAsString(fileInfo));
            }
        } catch (JsonProcessingException ex) {
            System.err.println("Could not map file info to JSON. Cause: " + ex);
        }
    }
}
