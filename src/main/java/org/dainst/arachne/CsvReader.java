package org.dainst.arachne;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 *
 * @author Reimar Grabowski
 */
public class CsvReader {

    private final ESService esService;
    
    private Map<String, Integer> indexMap = new HashMap<>();
    private int minLineLength;
    private boolean parsingErrors = false;
        
    int potentiallyInvalidDataLines = 0;
    int invalidDataLines = 0;
    
    public CsvReader(final ESService esService) {
        this.esService = esService;
    }
    
    public boolean read(final String path,  final boolean autoCorrect, final Set<String> ignoreFields
            , final boolean verbose) throws IOException {
        
        if (!(path.endsWith(".csv") || path.endsWith(".txt"))) {
            System.out.println("\rSkipping " + path + " (no csv or txt)");
            return false;
        }

        System.out.println("\rCatalog file: " + path);
        File file = new File(path);
        if (!file.canRead()) {
            System.err.println("Unable to read file: " + path);
            return false;
        }
        
        parsingErrors = false;
        potentiallyInvalidDataLines = 0;
        invalidDataLines = 0;
        
        List<String> columns = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(path), "UTF8"))) {
            
            // read header into list
            columns = reader.lines()
                    .findFirst()
                    .map(line -> Arrays.asList(line.split("\t", -1)))
                    .get();
        } catch (IOException e) {
            throw e;
        }
        
        // create index map
        indexMap = new HashMap<>();
        int maxIndex = -1;
        if (columns != null) {
            Map<String, List<String>> tokenMap = Mapping.getTokenMap();
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
        }
                        
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
                    .filter(line -> line.length >= minLength)
                    .findFirst()
                    .get();
            
            fileInfoList.add(getLineAsFileInfo(firstDataLine, dataLineNumber.intValue(), autoCorrect, ignoreFields));
            
            // read remaining data
            fileInfoList.addAll(reader.lines()
                    .peek(line -> dataLineNumber.incrementAndGet())
                    .map(line -> getLineAsFileInfo(line.split("\t", -1), dataLineNumber.intValue(), autoCorrect, ignoreFields))
                    .collect(Collectors.toList()));
        } catch (IOException e) {
            throw e;
        }
        
        if (potentiallyInvalidDataLines > 0) {
            System.out.println("\rFile '" + path + "' has " + potentiallyInvalidDataLines + " potentially invalid lines.");
        }
        
        if (parsingErrors) {
            System.out.println("\rFile '" + path + "' has " + invalidDataLines + " invalid lines.");
            System.out.println("\rNo data imported.");
            return false;
        }
        
        System.out.println("\r" + fileInfoList.size() + " records imported.");
        return true;
    }
    
    private ArchivedFileInfo getLineAsFileInfo(final String[] dataLine, final int lineNumber
            , final boolean autoCorrect, final Set<String> ignoreFields) {
        
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
                    if (!value.isEmpty() || ignoreFields.contains(fieldName)) {
                        setterMethod.invoke(fileInfo, value);
                    } else {
                        potentiallyInvalidDataLines++;
                        System.err.println("Potentially invalid data at line " + lineNumber);
                        System.err.println("No value for field '" + fieldName + "'");
                        System.err.println("" + lineNumber + ": " + Arrays.toString(dataLine));
                        System.err.println();
                    }
                }

                return fileInfo;
            } catch (InvocationTargetException ex) {
                detailMessage = "Could not set field '" + fieldName + "'. Cause: " 
                        + ex.getTargetException().getMessage();
            } catch (NoSuchFieldException | NoSuchMethodException | IllegalAccessException ex) {
                System.err.println("Failed to call setter " + setterName);
                System.err.println("Cause: " + ex);
                System.exit(10);
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
    
    private void esImport(final ArchivedFileInfo currentFile, final boolean verbose) {

        final ObjectMapper mapper = new ObjectMapper();

        try {
            byte[] jsonAsBytes = mapper.writeValueAsBytes(currentFile);
            if (verbose) {
                System.out.println(mapper.writeValueAsString(currentFile));
            }
            
            String id = esService.addToIndex(jsonAsBytes);
            if (id == null || id.isEmpty()) {
                System.err.println("Failed to add entry " + mapper.writeValueAsString(currentFile));
            }
        } catch (JsonProcessingException ex) {
            System.err.println("Failed to map entry to JSON " + currentFile);
            System.err.println("Cause: " + ex.getMessage());
        }

    }
}
