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

    private String catalog;
    private String volume;

    private String index;

    private String name;
    private String path;

    private String size;
    private long sizeInBytes;

    private String created;
    private String lastChanged;

    private String resourceType;

    public ArchivedFileInfo(final String index) {
        this.index = index;
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

    public ArchivedFileInfo setCreated(String created) throws DateTimeParseException {
        this.created = convertDateFormat(created);
        return this;
    }

    public String getLastChanged() {
        return lastChanged;
    }

    public ArchivedFileInfo setLastChanged(String lastChanged) throws DateTimeParseException {
        this.lastChanged = convertDateFormat(lastChanged);
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
        return Long.parseLong(sizeInBytes);
    }

    private String convertDateFormat(final String date) throws DateTimeParseException {
        LocalDateTime dateTime = null;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(""
                + "[dd.MM.yyyy[ HH:mm:ss]]"
                + "[yyyy-MM-dd[ HH:mm:ss]]"
        );
        
        try {
            dateTime = LocalDateTime.parse(date, formatter);
        } catch (DateTimeParseException e) {
            dateTime = LocalDate.parse(date, formatter).atStartOfDay();
        }
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
        return dateTime.format(outputFormatter);
    }
}
