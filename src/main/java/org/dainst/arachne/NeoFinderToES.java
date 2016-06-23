package org.dainst.arachne;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;

/*
 * Exit codes
 *
 * 0 success 
 * 1 unrecognized command line option 
 * 2 failed to parse command line 
 * 3 failed to create elasticsearch index 
 * 4 unknown field given for -I
 * 6 elasticsearch host not found
 * 7 could not connect to elasticsearch cluster
 */
/**
 *
 * @author Simon Hohl
 * @author Reimar Grabowski
 */
public class NeoFinderToES {

    private static String esIndexName = "marbildertivoli";

    private static String esClusterName = "elasticsearch";
    private static InetAddress esAddress;

    private static ESService esService;

    private static boolean scanMode = true;

    private static int mimeInfo = 1;

    private static int maxThreads = 5;
    
    private static Set<String> ignoreFields;

    private static final String newline = System.getProperty("line.separator");

    private static final int availableCPUs = Runtime.getRuntime().availableProcessors();

    private static boolean verbose = false;

    public static void main(String[] args) {

        esAddress = InetAddress.getLoopbackAddress();

        final Options options = new Options();
        options.addOption("h", "help", false, "print this message");
        options.addOption("c", "catalog", false, "read cdfinder/neofinder catalog files");
        options.addOption("n", "newindex", false, "create a new elasticsearch index (if an old one with the same name exists it "
                + "will be deleted");
        options.addOption("v", "verbose", false, "show JSON objects that are added to the index");
        options.addOption(Option.builder("a")
                .longOpt("address")
                .desc("the address of the elasticsearch index (omitting this the local loopback address will be used)")
                .hasArg()
                .argName("ADDRESS")
                .build());
        options.addOption(Option.builder("i")
                .longOpt("indexname")
                .desc("the name of the elasticsearch index (omitting this the name '"
                        + esIndexName + "' will be used)")
                .hasArg()
                .argName("NAME")
                .build());
        options.addOption(Option.builder("I")
                .longOpt("ignore")
                .desc("The fields to ignore potentially invalid data for:" + newline)
                .hasArgs()
                .valueSeparator(',')
                .argName("FIELDLIST")
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

        String address = "";
        List<String> argList = null;
        try {
            final CommandLineParser parser = new DefaultParser();
            final CommandLine cmd = parser.parse(options, args);
            argList = cmd.getArgList();
            if (!argList.isEmpty()) {
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
                    esIndexName = cmd.getOptionValue("i");
                }
                if (cmd.hasOption("t")) {
                    maxThreads = Integer.valueOf(cmd.getOptionValue("t"));
                    maxThreads = maxThreads <= availableCPUs * 5 ? maxThreads : availableCPUs * 5;
                }
                if (cmd.hasOption("m")) {
                    mimeInfo = Integer.valueOf(cmd.getOptionValue("m"));
                }
                if (cmd.hasOption("I")) {
                    ignoreFields = Arrays.stream(cmd.getOptionValues("I")).collect(Collectors.toSet());
                    Map<String, List<String>> tokenMap = Mapping.getTokenMap();
                    for (String field: ignoreFields) {
                        if (!tokenMap.containsKey(field)) {
                            System.out.println("Unknown field '" + field + "'.");
                            System.exit(4);
                        }
                    }
                }
            } else {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("neofindertoes [options] FILE_OR_DIRECTORY1 [FILE_OR_DIRECTORY2 "
                        + "[FILE_OR_DIRECTORY3] ...]\nOptions:", options);
                System.exit(0);
            }

            esService = new ESService(esAddress, esClusterName, esIndexName);
            System.out.println("Elasticsearch cluster: " + esAddress.toString() + " [" + esClusterName + ']');
            if (esService.isClusterAvailable()) {
                if (cmd.hasOption("n")) {
                    if (esService.indexExists()) {
                        esService.deleteIndex();
                    }
                    if (!esService.createIndex()) {
                        System.out.println("Failed to create elasticsearch index");
                        System.exit(3);
                    }
                    System.out.println("Adding to newly created index '" + esIndexName + "'\n");
                } else {
                    if (!esService.indexExists()) {
                        if (!esService.createIndex()) {
                            System.out.println("Failed to create elasticsearch index");
                            System.exit(3);
                        }
                    }
                    System.out.println("Adding to existing index '" + esIndexName + "'\n");
                }
            } else {
                System.exit(7);
            }
        } catch (ParseException ex) {
            if (ex instanceof UnrecognizedOptionException) {
                System.out.println(ex.getMessage());
                System.exit(1);
            }
            System.out.println("Failed to parse command line options.\n" + ex.getMessage());
            System.exit(2);
        } catch (UnknownHostException ex) {
            System.out.println("Host '" + address + "' not found.");
            System.exit(6);
        }

        final ProgressRotating progressIndicator = new ProgressRotating();
        for (String filename : argList) {
            try {
                File scanDirectory = new File(filename).getCanonicalFile();
                
                if (!scanDirectory.exists()) {
                    System.err.println("\rSource '" + filename + "' does not exist.");
                    continue;
                }

                if (!verbose && !progressIndicator.isAlive()) {
                    progressIndicator.start();
                }

                if (scanDirectory.isDirectory()) {
                    if (scanMode) {
                        new FileSystemScanner(esService).scan(scanDirectory, maxThreads, mimeInfo, verbose);
                    } else {
                        String[] files = scanDirectory.list();
                        for (final String file : files) {
                            new CsvReader(esService).read(scanDirectory + "/" + file, ignoreFields, verbose);
                        }
                    }
                } else {
                    if (!scanMode) {
                        new CsvReader(esService).read(scanDirectory.getAbsolutePath(), ignoreFields, verbose);
                    }
                }
            } catch (IOException ex) {
                System.err.println("\rCould not read '" + filename + "'.");
            }
        }
        
        esService.close();
        
        if (!verbose && progressIndicator.isAlive()) {
            progressIndicator.terminate();
        }
    }
}
