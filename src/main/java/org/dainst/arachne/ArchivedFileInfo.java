package org.dainst.arachne;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 *
 * @author Simon Hohl
 * @author Reimar Grabowski
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ArchivedFileInfo {

    private static final DateTimeFormatter INPUT_FORMATTER = DateTimeFormatter.ofPattern(""
                + "[dd.MM.yyyy[ HH:mm:ss]]"
                + "[yyyy-MM-dd[ HH:mm:ss]]"
                + "[MM/dd/yyyy[ HH:mm:ss]]"
        );
    
    private static final DateTimeFormatter OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
    
    private String catalog;
    private String volume;

    private String index;

    private String name;
    private String path;

    private String size;
    private long sizeInBytes;

    private String created = "";
    private String lastChanged = "";

    private String resourceType;

    private final boolean autoCorrect;

    public ArchivedFileInfo(final String index, final boolean autoCorrect) {
        this.index = index;
        this.autoCorrect = autoCorrect;
    }

    public String getCatalog() {
        return catalog;
    }

    public ArchivedFileInfo setCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public String getVolume() {
        return volume;
    }

    public ArchivedFileInfo setVolume(String volume) {
        this.volume = volume;
        return this;
    }

    public String getIndex() {
        return index;
    }

    public ArchivedFileInfo setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getName() {
        return name;
    }

    public ArchivedFileInfo setName(String name) {
        this.name = name;
        return this;
    }

    public String getPath() {
        return path;
    }

    public ArchivedFileInfo setPath(String path) {
        this.path = path;
        return this;
    }

    public String getSize() {
        return size;
    }

    public ArchivedFileInfo setSize(String size) throws NumberFormatException {
        this.size = size;
        sizeInBytes = getSizeInByteFromString(size);
        return this;
    }

    public String getCreated() {
        return created;
    }

    public ArchivedFileInfo setCreated(final String created) throws DateTimeParseException {
        this.created = convertDateFormat(created);
        // this 'if'-statement is just for documentation purpose
        if (autoCorrect) {
            // since we don't know the order in which 'created' and 'lastChanged' are set we just try both variants
            // if 'created' could not be parsed try to set it to 'lastChanged' if this contains a valid value
            if ("~".equals(this.created)) {
                if (lastChanged.length() > 1) {
                    this.created = lastChanged;
                }
            }
            // if 'lastChanged' could not be parsed and 'created' could
            if ("~".equals(lastChanged)) {
                lastChanged = this.created;
            }
            // if both could not be parsed
            if ("~".equals(this.created) && "~".equals(lastChanged)) {
                throw new DateTimeParseException("Autocorrection failed. As both date columns could not be parsed."
                        , created, 0);
            }
        }
        return this;
    }

    public String getLastChanged() {
        return lastChanged;
    }

    public ArchivedFileInfo setLastChanged(final String lastChanged) throws DateTimeParseException {
        this.lastChanged = convertDateFormat(lastChanged);
        // this 'if' is just for documentation purpose
        if (autoCorrect) {
            // since we don't know the order in which 'created' and 'lastChanged' are set we just try both variants
            // if 'lastChange' could not be parsed try to set it to 'created' if this contains a valid value
            if ("~".equals(this.lastChanged)) {
                if (created.length() > 1) {
                    this.lastChanged = created;
                }
            }
            // if 'created' could not be parsed and 'lastChanged' could
            if ("~".equals(created)) {
                created = this.lastChanged;
            }
            // if both could not be parsed
            if ("~".equals(this.lastChanged) && "~".equals(created)) {
                throw new DateTimeParseException("Autocorrection failed. As both date columns could not be parsed."
                        , lastChanged, 0);
            }
        }
        return this;
    }

    public String getResourceType() {
        return resourceType;
    }

    public ArchivedFileInfo setResourceType(String resourceType) {
        this.resourceType = resourceType;
        return this;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public String toString() {
        return getName() + ", " + getPath() + ", " + getSize() + ", " + getSizeInBytes() + ", " + getCreated() + ", "
                + getLastChanged() + ", " + getResourceType() + ", " + getVolume() + ", " + getCatalog() + ", "
                + getIndex();
    }

    private long getSizeInByteFromString(String size) throws NumberFormatException {
        String sizeInBytes = size.substring(size.indexOf("(") + 1);
        sizeInBytes = sizeInBytes.substring(0, sizeInBytes.indexOf(" B"));
        sizeInBytes = sizeInBytes.replace(".", "");
        sizeInBytes = sizeInBytes.replace(",", "");
        return Long.parseLong(sizeInBytes);
    }

    private String convertDateFormat(final String date) throws DateTimeParseException {
        LocalDateTime dateTime = null;
        
        try {
            dateTime = LocalDateTime.parse(date, INPUT_FORMATTER);
        } catch (DateTimeParseException e) {
            try {
                dateTime = LocalDate.parse(date, INPUT_FORMATTER).atStartOfDay();
            } catch (DateTimeParseException ex) {
                if (autoCorrect) {
                    return "~";
                }
                throw ex;
            }
        }
        return dateTime.format(OUTPUT_FORMATTER);
    }
}
