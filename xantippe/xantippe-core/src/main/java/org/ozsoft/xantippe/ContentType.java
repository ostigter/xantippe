package org.ozsoft.xantippe;

import java.util.HashSet;
import java.util.Set;

/**
 * A content type (MIME type) associated with a set of file extentions.
 * 
 * @author Oscar Stigter
 */
public class ContentType {
    
    /** MIME type for XML documents. */ 
    public static final ContentType XML = new ContentType("text/xml");
    
    /** MIME type for XML Schema (XSD) files. */ 
    public static final ContentType SCHEMA = new ContentType("text/xsd");
    
    /** MIME type for plain text files. */ 
    public static final ContentType TEXT = new ContentType("text/plain");
    
    /** MIME type for binary files. */ 
    public static final ContentType BINARY = new ContentType("application/octet-stream");
    
    /** The MIME type. */
    private final String mimeType;
    
    /** The associated file extentions. */
    private final Set<String> fileExtentions;
    
    public ContentType(String mimeType) {
        this.mimeType = mimeType;
        fileExtentions = new HashSet<String>();
    }
    
    public String getMimeType() {
        return mimeType;
    }
    
    public boolean isXml() {
        return mimeType.equals(XML);
    }
    
    public boolean isSchema() {
        return mimeType.equals(SCHEMA);
    }
    
    /* package */ Set<String> getFileExtentions() {
        return fileExtentions;
    }
    
    /* package */ void addFileExtention(String extention) {
        if (!fileExtentions.contains(extention)) {
            fileExtentions.add(extention);
        }
    }

}
