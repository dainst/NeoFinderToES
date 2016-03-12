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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

/*
 * Exit codes
 *
 * 0 success 
 * 1 unrecognized command line option 
 * 2 failed to parse command line 
 * 3 failed to create elasticsearch index 
 * 4 file or directory does not exist 
 * 5 io exception 
 * 6 elasticsearch host not found
 * 7 could not connect to elasticsearch cluster
 */
/**
 *
 * @author Simon Hohl
 * @author Reimar Grabowski
 */
public class NeoFinderToES {

    private static final String LOG_FILE = "errors.log";

    private static int inputLineCounter = 0;
    private static int fileCounter = 0;
    private static int skippedCounter = 0;

    private static int indexName;
    private static int indexPath;

    private static int indexSize;
    private static int indexType;

    private static int indexCreated;
    private static int indexChanged;

    private static int indexCatalog;
    private static int indexVolume;

    private static boolean isFirstLine = false;

    private static String targetIndexName = "marbildertivoli";

    private static String esClusterName = "elasticsearch";
    private static InetAddress esAddress;

    private static PrintWriter parserLog;

    private static ESService esService;

    private static boolean scanMode = true;

    private static int mimeInfo = 1;

    private static int maxThreads = 5;

    private static String newline = System.getProperty("line.separator");

    private static int availableCPUs = Runtime.getRuntime().availableProcessors();

    private static boolean verbose = false;

    public static void main(String[] args) {

        esAddress = InetAddress.getLoopbackAddress();

        try {
            parserLog = new PrintWriter(new FileOutputStream(LOG_FILE, true));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
        }

        final Options options = new Options();
        options.addOption("h", "help", false, "print this message");
        options.addOption("c", "catalog", false, "read cdfinder/neofinder catalog files");
        options.addOption("n", "newindex", false, "create a new elasticsearch index (if an old one with the same name exists it "
                + "will be deleted");
        options.addOption("v", "verbose", false, "show files being processed");
        options.addOption(Option.builder("a")
                .longOpt("address")
                .desc("the address of the elasticsearch index (omitting this the local loopback address will be used)")
                .hasArg()
                .argName("ADDRESS")
                .build());
        options.addOption(Option.builder("i")
                .longOpt("indexname")
                .desc("the name of the elasticsearch index (omitting this the name '"
                        + targetIndexName + "' will be used)")
                .hasArg()
                .argName("NAME")
                .build());
        options.addOption(Option.builder("e")
                .longOpt("esclustername")
                .desc("the name of the elasticsearch cluster (omitting this the default name 'elasticsearch' will be "
                        + "used)")
                .hasArg()
                .argName("NAME")
                .build());
        options.addOption(Option.builder("m")
                .longOpt("mimeType")
                .desc("the mime type fetch strategy to use:" + newline
                        + "0: no mime type information is fetched (default)" + newline
                        + "1: mime type is 'guessed' based on file extension" + newline
                        + "2: mime type is detected by inspecting the file (most accurate but slow)")
                .hasArg()
                .argName("STRATEGY")
                .build());
        options.addOption(Option.builder("t")
                .longOpt("threads")
                .desc("the maximum number of threads used for reading (the default value is the number of available "
                        + "CPUs/Cores)")
                .hasArg()
                .argName("MAX_THREADS")
                .build());

        String fileOrDirName = "";
        String address = "";
        try {
            final CommandLineParser parser = new DefaultParser();
            final CommandLine cmd = parser.parse(options, args);
            List<String> argList = cmd.getArgList();
            if (!argList.isEmpty()) {
                fileOrDirName = argList.get(cmd.getArgList().size() - 1);
                scanMode = !cmd.hasOption("c");
                verbose = cmd.hasOption("v");
                if (cmd.hasOption("a")) {
                    address = cmd.getOptionValue("a");
                    esAddress = InetAddress.getByName(address);
                }
                if (cmd.hasOption("e")) {
                    esClusterName = cmd.getOptionValue("e");
                }
                if (cmd.hasOption("i")) {
                    targetIndexName = cmd.getOptionValue("i");
                }
                if (cmd.hasOption("t")) {
                    maxThreads = Integer.valueOf(cmd.getOptionValue("t"));
                    maxThreads = maxThreads <= availableCPUs * 5 ? maxThreads : availableCPUs * 5;
                }
                if (cmd.hasOption("m")) {
                    mimeInfo = Integer.valueOf(cmd.getOptionValue("m"));
                }
            } else {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("neofindertoes FILE_OR_DIRECTORY", options);
                System.exit(0);
            }

            esService = new ESService(esAddress, esClusterName);
            System.out.println("Elasticsearch cluster: " + esAddress.toString() + " [" + esClusterName + ']');
            if (esService.isClusterAvailable()) {
                if (cmd.hasOption("n")) {
                    if (esService.indexExists(targetIndexName)) {
                        esService.deleteIndex(targetIndexName);
                    }
                    if (!esService.createIndex(targetIndexName)) {
                        System.out.println("Failed to create elasticsearch index");
                        System.exit(3);
                    }
                    System.out.println("Adding to newly created index '" + targetIndexName + "'");
                } else {
                    if (!esService.indexExists(targetIndexName)) {
                        if (!esService.createIndex(targetIndexName)) {
                            System.out.println("Failed to create elasticsearch index");
                            System.exit(3);
                        }
                    }
                    System.out.println("Adding to existing index '" + targetIndexName + "'");
                }
            } else {
                System.exit(7);
            }
        } catch (ParseException ex) {
            if (ex instanceof UnrecognizedOptionException) {
                System.out.println(ex.getMessage());
                System.exit(1);
            }
            Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, "failed to parse command line options", ex);
            System.exit(2);
        } catch (UnknownHostException ex) {
            System.out.println("Host '" + address + "' not found.");
            System.exit(6);
        }

        Indexer indexer = null;
        try {
            File scanDirectory = new File(fileOrDirName).getCanonicalFile();

            if (!scanDirectory.exists()) {
                System.out.println("Source '" + fileOrDirName + "' does not exist.");
                System.exit(4);
            }

            if (scanDirectory.isDirectory()) {
                if (scanMode) {
                    System.out.format("Scanning %s ...\n", scanDirectory);

                    BlockingQueue<ArchivedFileInfo> queue = new LinkedBlockingQueue<>();

                    indexer = new Indexer(scanDirectory, targetIndexName, esService, queue, verbose);
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

                    System.out.println("Done.");
                    long endTime = new Date().getTime();
                    String timeTaken = DurationFormatUtils.formatDuration((endTime - startTime), "HH:mm:ss");
                    System.out.println("Elapsed time: " + timeTaken);
                    try {
                        System.out.println("Indexed files: " + indexedFiles.get());
                    } catch (InterruptedException ex) {
                        Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException ex) {
                        Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    singleThreadExecutor.shutdownNow();
                } else {
                    String[] files = scanDirectory.list();
                    for (final String file : files) {
                        readCSV(scanDirectory + "/" + file);
                        printCSVStats();
                    }
                }
            } else {
                readCSV(scanDirectory.getAbsolutePath());
                printCSVStats();
            }
        } catch (IOException ex) {
            if (indexer != null) {
                indexer.terminate();
            }
            System.exit(5);
        }

        esService.close();
    }

    private static void printCSVStats() {
        System.out.println("Done. Lines processed: " + inputLineCounter);
        System.out.println(skippedCounter + " lines skipped");
        System.out.println(fileCounter + " entries");
    }

    private static void readCSV(String path) {

        if (!(path.endsWith(".csv") || path.endsWith(".txt"))) {
            System.out.println("Skipping " + path + " (no csv or txt)");
            return;
        }

        int currentColumns = -1;
        isFirstLine = true;
        System.out.println("New file: " + path);
        File file = new File(path);
        if (!file.canRead()) {
            System.out.println("Unable to read file: " + path);
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
                        parserLog.println("Possible additional tabs:");
                        parserLog.println(currentLine);
                        parserLog.println(lineContents.length + ", expected columns: " + currentColumns);
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
                                .setResourceType(currentType);

                        esImport(fileInfo);

                    } catch (ArrayIndexOutOfBoundsException e) {
                        parserLog.println("ArrayIndexOutOfBoundsException at line:");
                        parserLog.println(currentLine);
                        parserLog.println("Indices:");
                        parserLog.println(indexName + " " + indexPath + " " + indexSize + " "
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
    }

    private static void esImport(ArchivedFileInfo currentFile) {

        final ObjectMapper mapper = new ObjectMapper();

        try {
            byte[] jsonAsBytes = mapper.writeValueAsBytes(currentFile);

            String id = esService.addToIndex(targetIndexName, jsonAsBytes);
            if (id == null || id.isEmpty()) {
                Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, "Failed to add entry {0}", mapper.writeValueAsString(currentFile));
            }
        } catch (JsonProcessingException ex) {
            Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private static void setIndices(String[] lineContents) {

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

    private static long getSizeInByteFromString(String currentSize) throws NumberFormatException {
        String sizeInBtyes = currentSize.substring(currentSize.indexOf("(") + 1);
        sizeInBtyes = sizeInBtyes.substring(0, sizeInBtyes.indexOf(" B"));
        sizeInBtyes = sizeInBtyes.replace(".", "");
        return Long.parseLong(sizeInBtyes);
    }

    public static String convertDateFormat(final String date) {
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
