package org.dainst.arachne;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Reimar Grabowski
 */
public class Mapping {
    public static Map<String, List<String>> getTokenMap() {
        Map<String, List<String>> tokenMap = new HashMap<>();
        tokenMap.put("name", Arrays.asList("Name"));
        tokenMap.put("path", Arrays.asList("Pfad", "Path"));
        tokenMap.put("size", Arrays.asList("Größe", "Size"));
        tokenMap.put("created", Arrays.asList("Erstelldatum", "Date Created"));
        tokenMap.put("lastChanged", Arrays.asList("Änderungsdatum", "Date Modified"));
        tokenMap.put("resourceType", Arrays.asList("Art", "Kind", "Media-Info"));
        tokenMap.put("catalog", Arrays.asList("Katalog", "Catalog"));
        tokenMap.put("volume", Arrays.asList("Name des Volumes", "Volume"));
        return tokenMap;
    }
}
