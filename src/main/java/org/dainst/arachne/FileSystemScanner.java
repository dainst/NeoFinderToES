package org.dainst.arachne;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.LinkOption;
import java.util.Date;
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

    public void scan(final File scanDirectory, final int maxThreads, final int mimeInfo, final boolean verbose) 
            throws IOException {

        this.volume = scanDirectory.toString();
        this.verbose = verbose;
        FileInfoCollector fileInfoCollector = null;

        System.out.format("\rScanning %s...\n", scanDirectory);

        BlockingQueue<ArchivedFileInfo> queue = new LinkedBlockingQueue<>();

        fileInfoCollector = new FileInfoCollector(scanDirectory, esService, queue, verbose);
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(2);
        Future<Integer> indexedFiles = (Future<Integer>) fixedThreadPool.submit(fileInfoCollector);

        DirectoryCrawler crawler = new DirectoryCrawler(scanDirectory.toPath().toRealPath(LinkOption.NOFOLLOW_LINKS)
                , mimeInfo, verbose, queue);
        Future<Integer> readFiles = (Future<Integer>) fixedThreadPool.submit(crawler);

        long startTime = new Date().getTime();

        // clean up if the execution is finished or terminated (for example by ctrl+c)
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                fixedThreadPool.shutdownNow();
                esService.close();
            }
        });

        int filesRead = 0;
        try {
            filesRead = readFiles.get();
        } catch (InterruptedException | ExecutionException ignore) {};
                
        fileInfoCollector.interrupt(filesRead);

        int fileInfos = 0;
        try {
            fileInfos = indexedFiles.get();
        } catch (InterruptedException | ExecutionException ignore) {
        }

        if (fileInfos > 0) {
            System.out.println("\rDone.\n");

            long diff = new Date().getTime() - startTime;
            String timeTaken = String.format("%02d min, %02d sec", TimeUnit.MILLISECONDS.toMinutes(diff), TimeUnit.MILLISECONDS.toSeconds(diff)
                    - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(diff)));
            System.out.println("\rElapsed time: " + timeTaken);
        }
        fixedThreadPool.shutdown();
    }
}
