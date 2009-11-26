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

package org.ozsoft.xantippe;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ozsoft.xantippe.filestore.FileStore;
import org.ozsoft.xantippe.filestore.FileStoreException;

/**
 * A document stored in the database.
 * 
 * @author Oscar Stigter
 */
public class Document implements Comparable<Document> {
    
    /** Log */
    private static final Log LOG = LogFactory.getLog(Document.class);
    
    /** Buffer size for byte stream operations. */
    private static final int BUFFER_SIZE = 8192;  // 8 kB
    
    /** Back-reference to the database. */
    private final DatabaseImpl database;

    /** ID. */
    private final int id;
    
    /** Name. */
    private String name;
    
    /** Media type. */
    private final MediaType mediaType;
    
    /** Document length. */
    private int length = 0;
    
    /** Creation timestamp. */
    private final long created;
    
    /** Modification timestamp. */
    private long modified;
    
    /** Parent collection ID. */
    private int parent;
    
    /** Compression mode. */
    private CompressionMode compressionMode = CompressionMode.NONE;
    
    
    /* package */ Document(DatabaseImpl database, int id, String name,
            MediaType mediaType, long created, long modified, int parent) {
        this.database = database;
        this.id = id;
        this.name = name;
        this.mediaType = mediaType;
        this.created = created;
        this.modified = modified;
        this.parent = parent;
        
        FileStore fileStore = database.getFileStore();
        if (!fileStore.exists(id)) {
            try { 
                fileStore.create(id);
            } catch (FileStoreException e) {
                // Administrative error (should never happen).
                String msg = String.format(
                        "Error creating document '%s' in FileStore; ID not unique",
                        this);
                LOG.error(msg, e);
            }
        }
        
        database.addDocument(this);
    }
    
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
    
    public String getContentType() {
        return mediaType.getContentType();
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
        FileStore fileStore = database.getFileStore();
        
        database.getLockManager().lockRead(this);
        
        InputStream is = null;
        try {
            is = fileStore.retrieve(id);
            if (compressionMode == CompressionMode.DEFLATE) {
                is = new InflaterInputStream(is);
            }
        } catch (FileStoreException e) {
            String msg = String.format(
                    "Could not retrieve document '%s': %s",
                    this, e.getMessage());
            LOG.error(msg, e);
            throw new XmldbException(msg, e);
        } finally {
            database.getLockManager().unlockRead(this);
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
            String msg = String.format("Could not store document: '%s'", this);
            LOG.error(msg, e);
            throw new XmldbException(msg, e);
        }
    }
    
    /**
     * Sets the document's content based on a file.
     * 
     * In case of an XML document and depending on the validation mode of the
     * collection, the document may be validated against its schema before it
     * is stored.
     * 
     * In case of a schema it is parsed and registered by its target namespace.
     * 
     * Depending on the compression mode of the collection, the document may be
     * stored in compressed form.
     *  
     * @param   file  the file with the document's content
     * 
     * @throws  IllegalArgumentException  if the file is null 
     * 
     * @throws  XmldbException
     *              if the file does not exist, could not be read or parsed, or
     *              the document could not be compressed or stored
     */
    public void setContent(File file) throws XmldbException {
        if (file == null) {
            throw new IllegalArgumentException("Null file");
        }
        
        if (!file.isFile()) {
            String msg = "File not found: " + file;
            LOG.error(msg);
            throw new XmldbException(msg);
        }
        
        if (!file.canRead()) {
            String msg = "No read permission on file: " + file;
            LOG.error(msg);
            throw new XmldbException(msg);
        }
        
        Collection parentCol = getParent();
        
        if (mediaType == MediaType.XML) {
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
                    LOG.error("Invalid validation mode: " + vm);
            }
        } else if (mediaType == MediaType.SCHEMA) {
            try {
                database.getValidator().addSchema(file, id);
            } catch (Exception e) {
                String msg = String.format(
                        "Invalid schema file: '%s': %s", file, e.getMessage());
                LOG.error(msg);
                throw new XmldbException(msg);
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
                    LOG.error(msg);
                    throw new XmldbException(msg);
                }
                InputStream is = new FileInputStream(file);
                byte[] buffer = new byte[BUFFER_SIZE];                
                int read;
                while ((read = is.read(buffer)) > 0) {
                    os.write(buffer, 0, read);
                }
                os.close();
                is.close();
            } else {
                // No compression; store document as-is.
                storedFile = file;
            }
        	
            database.getLockManager().lockWrite(this);
            
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
        	LOG.error(msg, e);
        	throw new XmldbException(msg, e);
        	
        } catch (FileStoreException e) {
            String msg = String.format(
                    "Error while storing document '%s': %s",
                    this, e.getMessage());
            LOG.error(msg, e);
            throw new XmldbException(msg, e);
            
        } finally {
            database.getLockManager().unlockWrite(this);
            
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
     * @param   name   the key name
     * @param   value  the key value
     * 
     * @throws  IllegalArgumentException
     *              if the name is null or empty, or the value is null
     */
    public void setKey(String name, Object value) {
        if (name == null || name.length() == 0) {
            throw new IllegalArgumentException("Null or empty name");
        }
        if (value == null) {
            throw new IllegalArgumentException("Null value");
        }
        
        getParent().addIndexValue(name, value, id);
    }
    
    /**
     * Returns the hash code of this object.
     * 
     * @return  the hash code
     */
    @Override // Object
    public int hashCode() {
        return id;
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
     * Compares this document with the specified document.
     * 
     * Documents are compared based on their name (alphabetically).
     * 
     * @param  doc  the document to compare this document with
     * 
     * @return  the comparison value 
     */
    public int compareTo(Document doc) {
        return name.compareTo(doc.getName());
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
        LOG.debug(msg);
    }
    
    private void updateModified() {
        modified = System.currentTimeMillis();
    }
    
}
