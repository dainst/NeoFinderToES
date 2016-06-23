package org.dainst.arachne;

import java.io.File;
import java.io.IOException;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.time.DurationFormatUtils;

/**
 *
 * @author Reimar Grabowski
 */
public class FileSystemScanner {
    private final ESService esService;
        
    public FileSystemScanner(final ESService esService) {
        this.esService = esService;
    }
    
    public void scan(final File scanDirectory, final int maxThreads, final int mimeInfo, final boolean verbose
            ) throws IOException {
        
        Indexer indexer = null;
        try {
            System.out.format("\rScanning %s ...\n", scanDirectory);

            BlockingQueue<ArchivedFileInfo> queue = new LinkedBlockingQueue<>();

            indexer = new Indexer(scanDirectory, esService, queue, verbose);
            ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
            Future<Long> indexedFiles = (Future<Long>) singleThreadExecutor.submit(indexer);

            DirectoryCrawler crawler;

            crawler = new DirectoryCrawler(scanDirectory.toPath().toRealPath(LinkOption.NOFOLLOW_LINKS),
                    mimeInfo, queue);
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

            try {
                pool.invoke(crawler);
            } catch (CancellationException e) {
                // nothing to do here
            }

            while (!queue.isEmpty()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                    Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            indexer.terminate();

            System.out.println("\rDone.");
            long endTime = new Date().getTime();
            String timeTaken = DurationFormatUtils.formatDuration((endTime - startTime), "HH:mm:ss");
            System.out.println("\rElapsed time: " + timeTaken);
            try {
                System.out.println("\rIndexed files: " + indexedFiles.get());
            } catch (InterruptedException ex) {
                Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
                Thread.currentThread().interrupt();
            } catch (ExecutionException ex) {
                Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
            }
            singleThreadExecutor.shutdownNow();
        } catch (IOException ex) {
            if (indexer != null) {
                indexer.terminate();
            }
            throw ex;
        }
    }
}
