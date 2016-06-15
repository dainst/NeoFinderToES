package org.dainst.arachne;

import com.fasterxml.jackson.annotation.JsonInclude;

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

    public ArchivedFileInfo setSize(String size) {
        this.size = size;
        return this;
    }

    public String getCreated() {
        return created;
    }

    public ArchivedFileInfo setCreated(String created) {
        this.created = created;
        return this;
    }

    public String getLastChanged() {
        return lastChanged;
    }

    public ArchivedFileInfo setLastChanged(String lastChanged) {
        this.lastChanged = lastChanged;
        return this;
    }

    public String getResourceType() {
        return resourceType;
    }

    public ArchivedFileInfo setResourceType(String resourceType) {
        this.resourceType = resourceType;
        return this;
    }
    
    @Override
    public String toString() {
        return getName() + ", " + getPath() + ", " + getSize() + ", " + getCreated() + ", " + getLastChanged() + ", " 
                + getResourceType() + ", " + getVolume() + ", " + getCatalog() + ", " + getIndex();
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public ArchivedFileInfo setSizeInBytes(long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
        return this;
    }
}
