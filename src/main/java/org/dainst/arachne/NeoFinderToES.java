/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;

/**
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
    private static int indexType;

    private static int indexCreated;
    private static int indexChanged;

    private static int indexCatalog;
    private static int indexVolume;

    private static boolean isFirstLine = false;

    private static String targetIndexName = "marbildertivoli";

    private static PrintWriter parserLog;

    private static ESService esService;

    public static void main(String[] args) throws FileNotFoundException, ClassNotFoundException {

        parserLog = new PrintWriter(new FileOutputStream(LOG_FILE, true));

        final Options options = new Options();
        options.addOption("h", "help", false, "print this message");
        options.addOption("c", "catalog", false, "read cdfinder/neofinder catalog files");
        options.addOption("n", "newindex", false, "create a new elasticsearch index (if an old one with the same name exists it "
                + "will be deleted");
        options.addOption(Option.builder("i")
                .longOpt("indexname")
                .desc("the name of the elasticsearch index (omitting this the name '"
                        + targetIndexName + "' will be used)")
                .hasArg()
                .argName("NAME")
                .build());

        String fileOrDirName = "";
        try {
            final CommandLineParser parser = new DefaultParser();
            final CommandLine cmd = parser.parse(options, args);
            List<String> argList = cmd.getArgList();
            if (!argList.isEmpty()) {
                fileOrDirName = argList.get(cmd.getArgList().size() - 1);
            } else {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("neofindertoes FILE_OR_DIRECTORY", options);
                System.exit(0);
            }
        } catch (ParseException ex) {
            if (ex instanceof UnrecognizedOptionException) {
                System.out.println(ex.getMessage());
                System.exit(1);
            }
            Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, "failed to parse command line options", ex);
            System.exit(2);
        }

        File scanDirectory = new File(fileOrDirName);

        if (!scanDirectory.exists()) {
            System.out.println("Source '" + fileOrDirName + "' does not exist.");
            return;
        }
        
        esService = new ESService();
        if (!esService.createIndex(targetIndexName)) {
            System.out.println("Adding to index '" + targetIndexName + "'");
        }

        if (scanDirectory.isDirectory()) {
            String[] files = scanDirectory.list();
            for (final String file : files) {
                readCSV(scanDirectory + "/" + file);
            }
        } else {
            readCSV(scanDirectory.getAbsolutePath());
        }

        esService.close();

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

                    if (lineContents.length < 12) {
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
                        String currentType = (indexType != -1)
                                ? lineContents[indexType] : null;
                        String currentCatalog = (indexCatalog != -1)
                                ? lineContents[indexCatalog] : null;
                        String currentVolume = (indexVolume != -1)
                                ? lineContents[indexVolume] : null;

                        String currentCreated = (indexCreated != -1) ? lineContents[indexCreated] : null;
                        String currentChanged = (indexChanged != -1) ? lineContents[indexChanged] : null;

                        ArchivedFileInfo fileInfo = new ArchivedFileInfo(
                                currentName, currentPath,
                                currentCreated, currentChanged,
                                currentCatalog, currentVolume, currentType);

                        esImport(fileInfo);

                    } catch (ArrayIndexOutOfBoundsException e) {
                        parserLog.println("ArrayIndexOutOfBoundsException at line:");
                        parserLog.println(currentLine);
                        parserLog.println("Indices:");
                        parserLog.println(indexName + " " + indexPath + " "
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
            } else if (lineContents[i].compareTo("Erstelldatum") == 0 || lineContents[i].compareTo("Date Created") == 0) {
                indexCreated = i;
            } else if (lineContents[i].compareTo("Änderungsdatum") == 0 || lineContents[i].compareTo("Date Modified") == 0) {
                indexChanged = i;
            } else if (lineContents[i].compareTo("Art") == 0 || lineContents[i].compareTo("Kind") == 0) {
                indexType = i;
            } else if (lineContents[i].compareTo("Katalog") == 0 || lineContents[i].compareTo("Catalog") == 0) {
                indexCatalog = i;
            } else if (lineContents[i].compareTo("Volume") == 0) {
                indexVolume = i;
            }
        }
    }

}
