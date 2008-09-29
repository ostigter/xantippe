// This file is part of the Xantippe XML database.
//
// Copyright 2008 Oscar Stigter
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package xantippe;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.log4j.Logger;

import xantippe.filestore.FileStoreException;


/**
 * Document instance.
 * 
 * @author Oscar Stigter
 */
public class Document implements Comparable<Document> {
    
    
    /** log4j logger. */
    private static final Logger logger = Logger.getLogger(Document.class);
    
    /** Buffer size for byte stream operations. */
    private static final int BUFFER_SIZE = 8192;  // 8 kB
    
    /** Buffer for byte stream operations. */
    private final byte[] buffer = new byte[BUFFER_SIZE];
    
    /** Back-reference to the database. */
    private final DatabaseImpl database;

    /** ID. */
    private final int id;
    
    /** Name. */
    private String name;
    
    /** Media type. */
    private MediaType mediaType;
    
    /** Document length. */
    private int length = 0;
    
    /** Creation timestamp. */
    private long created;
    
    /** Modification timestamp. */
    private long modified;
    
    /** Parent collection ID. */
    private int parent;
    
    /** Compression mode. */
    private CompressionMode compressionMode;
    
    
    //------------------------------------------------------------------------
    //  Constructors
    //------------------------------------------------------------------------

    
    /* package */ Document(DatabaseImpl database, int id, String name,
            MediaType mediaType, long created, long modified, int parent) {
        this.database = database;
        this.id = id;
        this.name = name;
        this.mediaType = mediaType;
        this.created = created;
        this.modified = modified;
        this.parent = parent;
        
        database.addDocument(this);
    }
    
    
    //------------------------------------------------------------------------
    //  Public methods
    //------------------------------------------------------------------------

    
    /**
     * Returns the document's name.
     * 
     * @return  the document's name
     */
    public String getName() {
        return name;
    }
    
    
    /**
     * Returns the collection this document is stored in.
     * 
     * @return  the collection
     */
    public Collection getParent() {
        return database.getCollection(parent);
    }
    
    
    /**
     * Returns the document's media type.
     * 
     * @return  the media type
     */
    public MediaType getMediaType() {
        return mediaType;
    }
    
    
    /**
     * Returns the document's length in bytes.
     * 
     * @return  the length
     */
    public int getLength() {
        return length;
    }
    
    
    /**
     * Returns the document's stored length in bytes.
     * 
     * @return  the stored length
     */
    public int getStoredLength() {
        return database.getFileStore().getLength(id); 
    }
    
    
    /**
     * Returns the timestamp the document was created.
     * 
     * @return  the timestamp in milliseconds after 01-Jan-1970
     */
    public long getCreated() {
        return created;
    }
    
    
    /**
     * Returns the timestamp the document was last modified.
     * 
     * @return  the timestamp in milliseconds after 01-Jan-1970
     */
    public long getModified() {
        return modified;
    }
    
    
    /**
     * Returns the document's absolute URI.
     * 
     * @return  the URI
     */
    public String getUri() {
        Collection parent = getParent();
        StringBuilder sb = new StringBuilder();
        if (parent != null) {
            sb.append(parent.getUri());
        }
        sb.append('/');
        sb.append(name);
        return sb.toString(); 
    }
    
    
    /**
     * Returns the document's content.
     * 
     * @return  an InputStream to read the content from
     * 
     * @throws  XmldbException  if the contents could not be retrieved
     */
    public InputStream getContent() throws XmldbException {
        InputStream is = null;
        
        try {
            is = database.getFileStore().retrieve(id);
            if (compressionMode == CompressionMode.DEFLATE) {
                is = new InflaterInputStream(is);
            }
        } catch (FileStoreException e) {
            String msg = String.format(
                    "Could not retrieve content of document '%s': %s",
                    this, e.getMessage());
            logger.error(msg);
            throw new XmldbException(msg, e);
        }
        
        return is;
    }
    
    
    /**
     * Sets the document's content using an OutputStream.
     * The streamed document's content is written to a temporary file. 
     * When the client closes the stream, the document is stored using the
     * setContent(File) method.
     * 
     * @return  an OutputStream to write the document's content to
     * 
     * @throws XmldbException
     *             if the document is invalid, or it could not be stored
     */
    public OutputStream setContent() throws XmldbException {
        try {
            return new InsertStream(this);
        } catch (IOException e) {
            String msg = "Could not store document: " + this;
            logger.error(msg, e);
            throw new XmldbException(msg, e);
        }
    }
    
    
    /**
     * Sets the document's content based on a file.
     * 
     * Depending on the validation mode of the collection, the document may be
     * validated against its schema before it is stored.
     * 
     * Depending on the compression mode of the collection, the document may be
     * stored in compressed form.
     *  
     * @param  file  the file with the document's content
     * 
     * @throws  IllegalArgumentException  if the file is null 
     * 
     * @throws XmldbException
     *             if the file does not exist, could not be read or parsed, or
     *             the document could not be compressed or stored
     */
    public void setContent(File file) throws XmldbException {
        if (file == null) {
            throw new IllegalArgumentException("Null file");
        }
        
        if (!file.isFile()) {
            String msg = "File not found: " + file;
            logger.error(msg);
            throw new XmldbException(msg);
        }
        
        if (!file.canRead()) {
            String msg = "No permission to read file: " + file;
            logger.error(msg);
            throw new XmldbException(msg);
        }
        
        Collection parentCol = getParent();
        
        if (mediaType == MediaType.SCHEMA) {
            try {
                database.getValidator().addSchema(file, id);
            } catch (Exception e) {
                String msg = String.format(
                        "Invalid schema file: '%s': %s", file, e.getMessage());
                logger.error(msg);
                throw new XmldbException(msg);
            }
        } else if (mediaType == MediaType.XML) {
            String uri = getUri();
            ValidationMode vm = parentCol.getValidationMode(true);
            switch (vm) {
                case ON:
                    // Validate document against mandatory schema. 
                    database.getValidator().validate(file, uri, true);
                    break;
                case AUTO:
                    // Validate document if possible (namespace and schema).
                    database.getValidator().validate(file, uri, false);
                    break;
                case OFF:
                    // No validation.
                    break;
                default:
                    // Should never happen.
                    logger.error("Invalid validation mode: " + vm);
            }
        }
        
        // Use compression mode as configured for collection.
        compressionMode = parentCol.getCompressionMode(true);

        File storedFile = null;
        
        try {
            if (compressionMode != CompressionMode.NONE) {
                // Compress document to a temporary file.
                storedFile = File.createTempFile("xantippe-", null);
                OutputStream os;
                if (compressionMode == CompressionMode.DEFLATE) {
                    os = new DeflaterOutputStream(
                            new FileOutputStream(storedFile));
                } else {
                    // Should never happen.
                    String msg = "Invalid compression mode: " + compressionMode;
                    logger.error(msg);
                    throw new XmldbException(msg);
                }
                InputStream is = new FileInputStream(file);
                int length;
                while ((length = is.read(buffer, 0, buffer.length)) > 0) {
                    os.write(buffer, 0, length);
                }
                os.close();
                is.close();
            } else {
                // No compression; store document as-is.
                storedFile = file;
            }
        	
            database.getFileStore().store(id, storedFile);
            
            if (mediaType == MediaType.XML) {
            	parentCol.indexDocument(this, file);
            }
            
            length = (int) file.length();
            
            updateModified();
            
        } catch (IOException e) {
            String msg = String.format(
                    "Error while compressing document '%s': %s",
                    this, e.getMessage());
        	logger.error(msg, e);
        	throw new XmldbException(msg, e);
        	
        } catch (FileStoreException e) {
            String msg = String.format(
                    "Error while storing document '%s': %s",
                    this, e.getMessage());
            logger.error(msg, e);
            throw new XmldbException(msg, e);
            
        } finally {
            if (compressionMode != CompressionMode.NONE
                    && storedFile != null) {
                // Always clean up temporary (compressed) file.
                storedFile.delete();
            }
        }
    }
    
    
    /**
     * Sets a manual key value for this document.
     * 
     * An index value for this document is added to the collecting the document
     * is stored in.
     * 
     * @param  name   the key name
     * @param  value  the key value
     */
    public void setKey(String name, Object value) {
        Collection col = getParent();
        col.addIndexValue(name, value, id);
    }
    
    
    /**
     * Return true if this document is equal to the specified Object, otherwise
     * false.
     * 
     * @param  obj  the Object this document is compared to
     * 
     * @return  true if equal, otherwise false
     */
    @Override // Object
    public boolean equals(Object obj) {
        if (obj instanceof Document) {
            Document doc = (Document) obj;
            return doc.getId() == id;
            
        } else {
            return false;
        }
    }
    
    
    /**
     * Returns a string representation of this document.
     * 
     * A document is represented by its absolute URI.
     * 
     * @return  the string representation
     */
    @Override
    public String toString() {
        return getUri();
    }
    
    
    /**
     * Compares this document with the specified document.
     * 
     * Documents are compared based on their name (alphabetically).
     * 
     * @param  doc  the document to compare this document with
     * 
     * @return  the comparison value 
     */
    public int compareTo(Document doc) {
        //TODO: Maybe compare documents by URI (slower)?
        return name.compareTo(doc.getName());
    }
    

    //------------------------------------------------------------------------
    //  Package protected methods
    //------------------------------------------------------------------------
    
    
    /* package */ int getId() {
        return id;
    }
    
    
    /* package */ CompressionMode getCompressionMode() {
        return compressionMode;
    }
    
    
    /* package */ void setCompressionMode(CompressionMode compressionMode) {
        this.compressionMode = compressionMode;
    }
    
    
    /* package */ void delete() {
        database.deleteDocument(this);
        String msg = String.format("Deleted document '%s'", this);
        logger.debug(msg);
    }
    
    
    //------------------------------------------------------------------------
    //  Private methods
    //------------------------------------------------------------------------
    
    
    private void updateModified() {
        modified = System.currentTimeMillis();
    }

    
}
