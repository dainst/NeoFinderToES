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
import java.sql.SQLException;
import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Simon Hohl
 * @author Reimar Grabowski
 */
public class NeoFinderToES {

    private static final int RESULT_LINE_LIMIT = 5000000;

    private static final Pattern BAUWERK_PATTERN = Pattern.compile("^BT(\\d+)$");
    private static final Pattern ZERO_PATTERN = Pattern.compile("^0+(\\d+)$");
    private static final Pattern NUMBERS_PATTERN = Pattern.compile("\\d+");
    private static final Pattern FILENAME_PATTERN = Pattern.compile("^.*_(\\w+)(,\\d{2})?\\.\\w{3}$");

    private static final String OUTPUT_PATH = "results";
    private static final String LOG_FILE = "errors.log";

    private static final SimpleDateFormat DATE_FORMAT_GERMAN
            = new SimpleDateFormat("EEEE, dd. MMMM yyyy, HH:mm:ss", new DateFormatSymbols(Locale.GERMANY));
    private static final SimpleDateFormat DATE_FORMAT_ENGLISH
            = new SimpleDateFormat("EEEE, MMMM dd, yyyy, HH:mm:ss", new DateFormatSymbols(Locale.ENGLISH));
    private static final SimpleDateFormat OUTPUT_DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static int inputLineCounter = 0;
    private static int entityCounter = 0;
    private static int folderCounter = 0;
    private static int fileCounter = 0;
    private static int skippedCounter = 0;

    private static int resultLineCounter = 0;
    private static int resultFileCounter = 0;

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

    public static void main(String[] args) throws FileNotFoundException {

        parserLog = new PrintWriter(new FileOutputStream(LOG_FILE, true));

        System.out.println("Please specify target index name:");

        if (args.length != 1) {
            System.out.println("Expecting one parameter: arg[0] = source folder or file.");
            return;
        }

        File scanDirectory = new File(args[0]);

        if (!scanDirectory.exists()) {
            System.out.println("Source " + args[0] + " does not exist.");
            return;
        }

        if (scanDirectory.isDirectory()) {
            String[] files = scanDirectory.list();
            for (final String file : files) {
                if (file.endsWith(".csv")) {
                    readCSV(scanDirectory + "/" + file);
                }
            }
        } else if (scanDirectory.getName().endsWith(".csv")) {
            readCSV(scanDirectory.getAbsolutePath());
        }

        System.out.println("Done. Lines processed: " + inputLineCounter);
        System.out.println(skippedCounter + " lines skipped");
        System.out.println(folderCounter + " folders");
        System.out.println(fileCounter + " files");
        System.out.println(entityCounter + " of those files potentially referring to arachne entities");
    }

    private static void readCSV(String path) {
        int currentColumns = -1;
        isFirstLine = true;
        resultFileCounter = 0;
        resultLineCounter = 0;
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

                    if (lineContents[indexType].compareTo("Ordner") == 0 || lineContents[indexType].compareTo("Folder") == 0) {
                        folderCounter++;
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

                        String currentCreated = (indexCreated != -1) ? TryParsingDate(lineContents[indexCreated], currentLine) : null;
                        String currentChanged = (indexChanged != -1) ? TryParsingDate(lineContents[indexChanged], currentLine) : null;

                        ArachneEntity entityInfo = null;

                        if ((currentPath.toLowerCase().contains("datenbank") || currentPath.toLowerCase().contains("rohscan"))
                                || currentPath.toLowerCase().contains("druck")) {
                            entityInfo = tryParsingArachneEntityFromFileName(lineContents[indexName]);
                        } else {
                        }

                        String currentArachneID = (entityInfo != null) ? entityInfo.arachneID : null;
                        String currentRestrictingTable = (entityInfo != null) ? entityInfo.restrictingTable : null;
                        boolean currentForeignKey = (entityInfo != null) ? entityInfo.foreignKey : false;

                        ArchivedFileInfo fileInfo = new ArchivedFileInfo(
                                currentArachneID, currentName, currentPath,
                                currentCreated, currentChanged,
                                currentForeignKey, currentRestrictingTable,
                                currentCatalog, currentVolume, currentType);

                        ArrayList<ArchivedFileInfo> list = new ArrayList<>();

                        list.add(fileInfo);

                        if (resultLineCounter == RESULT_LINE_LIMIT) {
                            resultFileCounter++;
                            resultLineCounter = 0;
                        } else {
                            resultLineCounter++;
                        }

                        esImport(list);
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

    private static void esImport(List<ArchivedFileInfo> archivedFileInfoList) {

        try {

            final ESService esService = new ESService();
            if (!esService.createIndex(targetIndexName)) {
                    System.out.println("Adding to index '" + targetIndexName + "'");
            }

            DBService dbService = new DBService();
            String updateString = new String();
            final ObjectMapper mapper = new ObjectMapper();

            for (ArchivedFileInfo currentFile : archivedFileInfoList) {
                {
                    String folderType = new String();

                    if (currentFile.getPath().toLowerCase().contains("rohscan")) {
                        folderType = "Rohscan";
                    } else if (currentFile.getPath().toLowerCase()
                            .contains("datenbank")) {
                        folderType = "datenbankfertig";
                    } else if (currentFile.getPath().toLowerCase()
                            .contains("druckfertig")) {
                        folderType = "druckfertig";
                    } else {
                        folderType = "unbekannt";
                    }

                    String arachneEntityID = new String();
                    String dateinameTivoli = null;

                    if (currentFile.getArachneID() == null) {
                        arachneEntityID = null;
                    } else {
                        if (currentFile.isForeignKey()
                                && currentFile.getForcedTable() != null) {
                            String sql = "(SELECT `ArachneEntityID` "
                                    + "FROM `arachneentityidentification` "
                                    + " WHERE `TableName` = '" + currentFile.getForcedTable() + "' "
                                    + " AND `ForeignKey` = " + Long.parseLong(currentFile.getArachneID().replace("BT", ""))
                                    + " HAVING COUNT(*) = 1)";
                            try {
                                currentFile.setArachneID(String.valueOf(dbService.queryDBForLong(sql)));
                            } catch (SQLException ex) {
                                Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        } else if (currentFile.isForeignKey()) {
                            String sql = "(SELECT `ArachneEntityID` FROM `arachneentityidentification` "
                                    + " WHERE `ForeignKey` = " + Long.parseLong(currentFile.getArachneID().replace("BT", ""))
                                    + " HAVING COUNT(*) = 1)";
                            try {
                                currentFile.setArachneID(String.valueOf(dbService.queryDBForLong(sql)));
                            } catch (SQLException ex) {
                                Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }

                        String sql = "(SELECT `DateinameMarbilder` FROM `marbilder` "
                                + "WHERE `DateinameMarbilder`='" + currentFile.getName().replace("tif", "jpg") + "')";
                        try {
                            currentFile.setFileNameMarbilderTivoli(dbService.queryDBForString(sql));
                        } catch (SQLException ex) {
                            Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }

                    try {
                        byte[] jsonAsBytes = mapper.writeValueAsBytes(currentFile);

                        String id = esService.addToIndex(targetIndexName, jsonAsBytes);
                        if (id == null || id.isEmpty()) {
                            Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, "Failed to add entry {0}"
                                    , mapper.writeValueAsString(currentFile));
                        }
                    } catch (JsonProcessingException ex) {
                        Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }

            }
            try {
                dbService.close();
            } catch (SQLException ex) {
                Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
            }
            esService.close();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static ArachneEntity tryParsingArachneEntityFromFileName(
            String fileName) {
        Matcher matchFile = FILENAME_PATTERN.matcher(fileName);

        if (matchFile.matches()) {
            Matcher bauwerkMatcher = BAUWERK_PATTERN.matcher(matchFile.group(1));
            if (bauwerkMatcher.matches()) {
                entityCounter++;
                return new ArachneEntity(bauwerkMatcher.group(1), true,
                        "bauwerksteil");
            }

            Matcher zeroMatcher = ZERO_PATTERN.matcher(matchFile.group(1));
            if (zeroMatcher.matches()) {
                entityCounter++;
                return new ArachneEntity(zeroMatcher.group(1), true, null);
            }

            Matcher numbersMatcher = NUMBERS_PATTERN.matcher(matchFile.group(1));

            if (numbersMatcher.matches()) {
                entityCounter++;
                return new ArachneEntity(matchFile.group(1), false, null);
            }
        }
        return null;
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

    private static String TryParsingDate(String input, String originalLine) {
        if (input == null
                || input.compareTo("n.v.") == 0
                || input.compareTo("n.a.") == 0
                || input.compareTo("") == 0) {
            return null;
        } else {
            try {
                return OUTPUT_DATE_TIME_FORMAT.format(DATE_FORMAT_GERMAN.parse(input));
            } catch (ParseException e) {
                try {
                    return OUTPUT_DATE_TIME_FORMAT.format(DATE_FORMAT_ENGLISH.parse(input));
                } catch (ParseException ex) {
                    Logger.getLogger(NeoFinderToES.class.getName()).log(Level.SEVERE, null, ex);
                    parserLog.println("Unable to parse date:");
                    parserLog.println(input);
                    parserLog.println(originalLine);
                    return null;
                }
            }
        }
    }
}
