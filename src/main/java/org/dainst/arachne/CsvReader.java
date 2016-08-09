package org.dainst.arachne;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 *
 * @author Reimar Grabowski
 */
public class CsvReader {

    private final ProgressRotating progressIndicator;

    private final ESService esService;

    private final BulkIndexer bulkIndexer;

    private Map<String, Integer> indexMap = new HashMap<>();
    private int minLineLength;
    private boolean parsingErrors = false;

    int potentiallyInvalidDataLines = 0;
    int invalidDataLines = 0;
    int lostLines = 0;

    private final boolean verbose;

    private final Set<String> parsedIds = new HashSet<>();
    
    private Path tmpFile;
    
    public CsvReader(final ESService esService, final boolean verbose, final ProgressRotating progressIndicator) {
        this.esService = esService;
        this.verbose = verbose;
        this.progressIndicator = progressIndicator;
        bulkIndexer = new BulkIndexer(esService, verbose);
    }

    public boolean read(String path, final boolean autoCorrect, final Set<String> ignoreFields, final boolean minimal) throws IOException {

        if (!(path.endsWith(".csv") || path.endsWith(".txt"))) {
            System.out.println("\rSkipping " + path + " (no csv or txt)");
            return false;
        }

        System.out.println("\rCatalog file: " + path);
        Path file = Paths.get(path);
        if (!Files.isReadable(file)) {
            System.err.println("Unable to read file: " + path);
            return false;
        }

        parsingErrors = false;
        potentiallyInvalidDataLines = 0;
        invalidDataLines = 0;

        System.out.println("Cleaning file...");
        String out = "/tmp/" + Paths.get(path.substring(0, path.lastIndexOf(".")) + ".tmp.txt").getFileName();
        tmpFile = Paths.get(out);
        Files.deleteIfExists(tmpFile);
        try (BufferedReader charReader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF8"))) {
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out), "UTF-8"))) {
                int r;
                while ((r = charReader.read()) != -1) {
                    if (r != 10) {
                        writer.write(r);
                    }
                }
            } catch (IOException e) {
                throw e;
            }
        } catch (IOException e) {
            throw e;
        }

        path = out;

        List<String> columns = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(path), "UTF-8"))) {

            // read header into list
            columns = reader.lines()
                    .findFirst()
                    .map(line -> Arrays.asList(line.split("\t", -1)))
                    .get();
        } catch (IOException e) {
            throw e;
        }
        int headerSize = columns.size();
        // create index map
        indexMap = new HashMap<>();
        int maxIndex = -1;
        Map<String, List<String>> tokenMap;
        if (!minimal) {
            tokenMap = Mapping.getTokenMap();
        } else {
            tokenMap = Mapping.getMinimalTokenMap();
        }

        Iterator<Map.Entry<String, List<String>>> iterator = tokenMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<String>> next = iterator.next();
            for (String token : next.getValue()) {
                String key = next.getKey();
                if (columns.contains(token)) {
                    int columnIndex = columns.indexOf(token);
                    maxIndex = maxIndex < columnIndex ? columnIndex : maxIndex;
                    indexMap.put(key, columnIndex);
                    System.out.println("\rColumn providing field '" + key + "': " + columns.get(columnIndex));
                    break;
                }
            }
        }
        System.out.println();

        if (indexMap.keySet().size() != tokenMap.keySet().size()) {
            System.err.println("Invalid header: " + columns);
            throw new IOException("Invalid header.");
        }

        System.out.println("\rParsing...");
        // read data
        final int minLength = maxIndex + 1;
        minLineLength = minLength;
        List<ArchivedFileInfo> fileInfoList = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(path), "UTF8"))) {

            // little hack to 'get access' to line numbers
            final AtomicInteger dataLineNumber = new AtomicInteger(1);
            // find first line of data
            final String[] firstDataLine = reader.lines()
                    .skip(1)
                    .map(line -> line.split("\t", -1))
                    .peek(line -> dataLineNumber.incrementAndGet())
                    .filter(line -> line.length == headerSize)
                    .findFirst()
                    .get();

            fileInfoList.add(getLineAsFileInfo(firstDataLine, dataLineNumber.intValue(), autoCorrect, ignoreFields));

            // read remaining data
            if (!autoCorrect) {
                fileInfoList.addAll(reader.lines()
                        .peek(line -> dataLineNumber.incrementAndGet())
                        .map(line -> getLineAsFileInfo(line.split("\t", -1), dataLineNumber.intValue(), autoCorrect, ignoreFields))
                        .collect(Collectors.toList()));
            } else {
                List<String> rawLineData = reader.lines().collect(Collectors.toList());

                List<String> correctedData = new ArrayList<>();
                String correctedLine = "";
                int lineNumber = dataLineNumber.intValue();
                for (String line : rawLineData) {
                    lineNumber++;
                    int count = getTabs(line) + 1;
                    if (count == headerSize) {
                        correctedData.add(line);
                    } else {
                        if (verbose) {
                            System.err.println("Expected " + headerSize + ", got " + count + " columns.");
                            System.err.println("\r" + lineNumber + ": " + line);
                        }
                        if (count > headerSize) {
                            pauseProgressIndicator();
                            System.out.println("\rColumn mismatch at line " + lineNumber);
                            showLineMapped(line);
                            System.out.println("\r");
                            System.out.println("Import this file info and continue? [Y/n]");
                            Scanner scanner = new Scanner(System.in);
                            String confirm = scanner.nextLine();
                            if (confirm.isEmpty() || confirm.toLowerCase().startsWith("y")) {
                                correctedData.add(line);
                                unpauseProgresIndicator();
                                continue;
                            }
                            exit(9);
                        }
                        if ("".equals(correctedLine)) {
                            correctedLine = line;
                        } else {
                            correctedLine += line;
                            if (verbose) {
                                System.out.println("\rCorrected :");
                                int l = (int) Math.log10(lineNumber) + 1;
                                System.out.println("\r" + new String(new char[l]).replace('\0', '*') + ": "
                                        + correctedLine);
                            }
                            count = getTabs(correctedLine) + 1;
                            if (count == headerSize) {
                                correctedData.add(correctedLine);
                                correctedLine = "";
                                if (verbose) {
                                    System.out.println("\rAuto correction successfull.");
                                    System.out.println("");
                                }
                            } else {
                                if (count > headerSize) {
                                    System.err.println("Auto correction failed at line: " + lineNumber);
                                    exit(0);
                                }
                            }
                        }
                    }
                }
                fileInfoList.addAll(correctedData.stream()
                        .peek(line -> dataLineNumber.incrementAndGet())
                        .map(line -> getLineAsFileInfo(line.split("\t", -1), dataLineNumber.intValue(), autoCorrect, ignoreFields))
                        .collect(Collectors.toList()));
            }
            System.out.println("\rRecords parsed: " + fileInfoList.size() + "\n");
        } catch (IOException e) {
            throw e;
        }

        // delete tmp file
        Files.deleteIfExists(tmpFile);
                
        if (potentiallyInvalidDataLines > 0) {
            System.out.println("\rFile '" + path + "' has " + potentiallyInvalidDataLines + " potentially invalid lines.");
        }

        if (parsingErrors || (potentiallyInvalidDataLines > 0)) {
            System.out.println("\rFile '" + path + "' has " + invalidDataLines + " invalid lines.");
            System.out.println("\rNo data imported.");
            return false;
        }

        System.out.println("\rImporting into elasticsearch index...");
        fileInfoList.stream().forEach((fileInfo) -> {
            bulkIndexer.add(fileInfo, fileInfoList.size());
        });
        bulkIndexer.close(fileInfoList.size());
        if (lostLines > 0) {
            System.out.println("\r" + lostLines + " records lost.");
        }
        System.out.println("\r" + (fileInfoList.size() - lostLines) + " records imported.");
        return true;
    }

    private int getTabs(final String string) {
        AtomicInteger result = new AtomicInteger(0);
        string.chars().forEach(c -> {
            if (c == '\t') {
                result.addAndGet(1);
            }
        });
        return result.get();
    }

    private void showLineMapped(final String line) {
        System.out.println("Extracted file info: ");
        String[] columns = line.split("\t", -1);
        String result = "";
        for (Map.Entry<String, Integer> entrySet : indexMap.entrySet()) {
            String fieldName = entrySet.getKey();
            Integer index = entrySet.getValue();
            System.out.println(fieldName + ": " + columns[index]);
        }
    }

    private ArchivedFileInfo getLineAsFileInfo(final String[] dataLine, final int lineNumber, final boolean autoCorrect, final Set<String> ignoreFields) {

        String detailMessage = "\rMissing columns";
        if (dataLine.length >= minLineLength) {
            final ArchivedFileInfo fileInfo = new ArchivedFileInfo(esService.getIndexName(), autoCorrect);
            String setterName = "";
            String fieldName = "";
            try {
                for (Map.Entry<String, Integer> entrySet : indexMap.entrySet()) {
                    fieldName = entrySet.getKey();
                    Integer index = entrySet.getValue();
                    Field field = ArchivedFileInfo.class.getDeclaredField(fieldName);
                    Class<?> fieldType = field.getType();

                    setterName = "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
                    Method setterMethod = ArchivedFileInfo.class.getMethod(setterName, fieldType);

                    final String value = dataLine[index];
                    boolean ignore = ignoreFields.contains(fieldName);
                    if (!value.isEmpty() || ignore) {
                        try {
                            setterMethod.invoke(fileInfo, value);
                        } catch (InvocationTargetException e) {
                            // a little hack to get -A and -I working together for the date fields
                            if (e.getTargetException() instanceof DateTimeParseException
                                    && e.getTargetException().getMessage().equals(
                                            "Autocorrection failed. As both date columns could not be parsed.")) {
                                for (String fn : Arrays.asList(new String[]{"created", "lastChanged"})) {
                                    Field f = ArchivedFileInfo.class.getDeclaredField(fn);
                                    f.setAccessible(true);
                                    f.set(fileInfo, "");
                                }
                                continue;
                            }

                            if (ignore) {
                                continue;
                            }

                            throw e;
                        }
                    } else {
                        potentiallyInvalidDataLines++;
                        System.err.println("Potentially invalid data at line " + lineNumber);
                        System.err.println("No value for field '" + fieldName + "'");
                        System.err.println("" + lineNumber + ": " + Arrays.toString(dataLine));
                        System.err.println();
                        if ("path".equals(fieldName)) {
                            System.err.println("FATAL! Cannot import file info without path!");
                            exit(11);
                        }
                    }
                }

                String id = fileInfo.getPath();
                if (parsedIds.contains(id)) {
                    System.err.println("SEVERE! Dublicate path '" + fileInfo.getPath() + "'at line " + lineNumber);
                    System.err.println("" + lineNumber + ": " + Arrays.toString(dataLine));
                    System.out.println("Hit [ENTER] to exit.");
                    System.out.println("To continue the import type: Yes, I know, I will lose data!");
                    Scanner scanner = new Scanner(System.in);
                    String confirm = scanner.nextLine();
                    if (!confirm.equals("Yes, I know, I will lose data!")) {
                        exit(5);
                    }
                    System.out.println("");
                    lostLines++;
                } else {
                    parsedIds.add(id);
                }

                return fileInfo;
            } catch (InvocationTargetException ex) {
                detailMessage = "Could not set field '" + fieldName + "'. Cause: "
                        + ex.getTargetException().getMessage();
            } catch (NoSuchFieldException | NoSuchMethodException | IllegalAccessException ex) {
                System.err.println("Failed to call setter " + setterName);
                System.err.println("Cause: " + ex);
                exit(10);
            }
        }
        invalidDataLines++;
        System.err.println("Invalid data at line " + lineNumber);
        System.err.println(detailMessage);
        System.err.println(lineNumber + ": " + Arrays.toString(dataLine));
        System.err.println();
        parsingErrors = true;
        return null;
    }

    private void pauseProgressIndicator() {
        if (progressIndicator.isAlive()) {
            progressIndicator.pause();
        }
    }

    private void unpauseProgresIndicator() {
        if (progressIndicator.isAlive()) {
            progressIndicator.unpause();
        }
    }
    
    private void exit(final int exitCode) {
        try {
            Files.deleteIfExists(tmpFile);
        } catch (IOException ex) {
            System.err.println("Could not delete temporary file: " + tmpFile.toString());
        }
        System.exit(exitCode);
    }
}
