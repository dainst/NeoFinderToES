package org.dainst.arachne;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

/**
 *
 * @author Reimar Grabowski
 */
public class DataImporter {

    private ESService esService;
    
    private static final String LOG_FILE = "errors.log";
    
    private PrintWriter parserLog;

    private int inputLineCounter = 0;
    private int fileCounter = 0;
    private int skippedCounter = 0;

    private int indexName;
    private int indexPath;

    private int indexSize;
    private int indexType;

    private int indexCreated;
    private int indexChanged;

    private int indexCatalog;
    private int indexVolume;

    private static boolean isFirstLine = false;
    
    public DataImporter(final ESService esService) {
        this.esService = esService;
        try {
            parserLog = new PrintWriter(new FileOutputStream(LOG_FILE, true));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void scanFileSystem(final File scanDirectory, final int maxThreads, final int mimeInfo, final boolean verbose
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

    public void readCSV(final String path, final boolean verbose) {
        if (!(path.endsWith(".csv") || path.endsWith(".txt"))) {
            System.out.println("\rSkipping " + path + " (no csv or txt)");
            return;
        }

        int currentColumns = -1;
        isFirstLine = true;
        System.out.println("\rCatalog file: " + path);
        File file = new File(path);
        if (!file.canRead()) {
            System.out.println("\rUnable to read file: " + path);
            return;
        }

        try {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(path), "UTF8"))) {

                String currentLine;
                while ((currentLine = reader.readLine()) != null) {
                    inputLineCounter++;
                    String[] lineContents = currentLine.split("\\t", -1);

                    for (int i = 0; i < lineContents.length; i++) {
                        lineContents[i] = lineContents[i].trim();
                    }

                    if (lineContents.length < 8) {
                        skippedCounter++;
                        continue;
                    }
                    if (lineContents[0].compareTo("Name") == 0 && isFirstLine) {
                        isFirstLine = false;
                        currentColumns = lineContents.length;
                        setIndices(lineContents);
                        continue;
                    }

                    if (lineContents.length > currentColumns) {
                        parserLog.println("\rPossible additional tabs:");
                        parserLog.println("\r" + currentLine);
                        parserLog.println("\r" + lineContents.length + ", expected columns: " + currentColumns);
                        continue;
                    }

                    fileCounter++;
                    try {
                        String currentName = (indexName != -1)
                                ? lineContents[indexName] : null;
                        String currentPath = (indexPath != -1)
                                ? lineContents[indexPath] : null;
                        String currentSize = (indexSize != -1)
                                ? lineContents[indexSize] : null;
                        String currentType = (indexType != -1)
                                ? lineContents[indexType] : null;
                        String currentCatalog = (indexCatalog != -1)
                                ? lineContents[indexCatalog] : null;
                        String currentVolume = (indexVolume != -1)
                                ? lineContents[indexVolume] : null;

                        String currentCreated = (indexCreated != -1) ? lineContents[indexCreated] : null;
                        String currentChanged = (indexChanged != -1) ? lineContents[indexChanged] : null;

                        ArchivedFileInfo fileInfo = new ArchivedFileInfo()
                                .setName(currentName)
                                .setPath(currentPath)
                                .setSizeInBytes(getSizeInByteFromString(currentSize))
                                .setSize(currentSize)
                                .setCreated(convertDateFormat(currentCreated))
                                .setLastChanged(convertDateFormat(currentChanged))
                                .setCatalog(currentCatalog)
                                .setVolume(currentVolume)
                                .setIndex(esService.getIndexName())
                                .setResourceType(currentType);

                        esImport(fileInfo, verbose);

                    } catch (ArrayIndexOutOfBoundsException e) {
                        parserLog.println("\rArrayIndexOutOfBoundsException at line:");
                        parserLog.println("\r" + currentLine);
                        parserLog.println("\rIndices:");
                        parserLog.println("\r" + indexName + " " + indexPath + " " + indexSize + " "
                                + indexCreated + " " + indexChanged + " "
                                + indexType + " " + indexCatalog + " "
                                + indexVolume);
                    }
                }
            }
            parserLog.close();
        } catch (IOException | java.lang.NumberFormatException e) {
            Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, e);
        }
        printCSVStats();
    }

    public void esImport(final ArchivedFileInfo currentFile, final boolean verbose) {

        final ObjectMapper mapper = new ObjectMapper();

        try {
            byte[] jsonAsBytes = mapper.writeValueAsBytes(currentFile);
            if (verbose) {
                System.out.println(mapper.writeValueAsString(currentFile));
            }
            
            String id = esService.addToIndex(jsonAsBytes);
            if (id == null || id.isEmpty()) {
                Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, "Failed to add entry {0}", mapper.writeValueAsString(currentFile));
            }
        } catch (JsonProcessingException ex) {
            Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private void printCSVStats() {
        System.out.println("\rDone. Lines processed: " + inputLineCounter);
        System.out.println("\r" + skippedCounter + " lines skipped");
        System.out.println("\r" + fileCounter + " entries");
    }
    
    private void setIndices(String[] lineContents) {

        indexName = -1;
        indexPath = -1;

        indexSize = -1;

        indexCreated = -1;
        indexChanged = -1;

        indexType = -1;

        indexCatalog = -1;
        indexVolume = -1;

        for (int i = 0; i < lineContents.length; i++) {
            if (lineContents[i].compareTo("Name") == 0) {
                indexName = i;
            } else if (lineContents[i].compareTo("Pfad") == 0 || lineContents[i].compareTo("Path") == 0) {
                indexPath = i;
            } else if (lineContents[i].compareTo("Größe") == 0 || lineContents[i].compareTo("Size") == 0) {
                indexSize = i;
            } else if (lineContents[i].compareTo("Erstelldatum") == 0 || lineContents[i].compareTo("Date Created") == 0) {
                indexCreated = i;
            } else if (lineContents[i].compareTo("Änderungsdatum") == 0 || lineContents[i].compareTo("Date Modified") == 0) {
                indexChanged = i;
            } else if (lineContents[i].compareTo("Art") == 0 || lineContents[i].compareTo("Kind") == 0
                    || lineContents[i].compareTo("Media-Info") == 0) {
                indexType = i;
            } else if (lineContents[i].compareTo("Katalog") == 0 || lineContents[i].compareTo("Catalog") == 0) {
                indexCatalog = i;
            } else if (lineContents[i].compareTo("Volume") == 0) {
                indexVolume = i;
            }
        }
    }

    private long getSizeInByteFromString(String currentSize) throws NumberFormatException {
        String sizeInBtyes = currentSize.substring(currentSize.indexOf("(") + 1);
        sizeInBtyes = sizeInBtyes.substring(0, sizeInBtyes.indexOf(" B"));
        sizeInBtyes = sizeInBtyes.replace(".", "");
        return Long.parseLong(sizeInBtyes);
    }

    private String convertDateFormat(final String date) {
        try {
            DateTime dateTime = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss")
                    .parseDateTime(date)
                    .withZone(DateTimeZone.getDefault());

            return DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss").print(dateTime);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
