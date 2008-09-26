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

    
    public String getName() {
        return name;
    }
    
    
    public Collection getParent() {
        return database.getCollection(parent);
    }
    
    
    public MediaType getMediaType() {
        return mediaType;
    }
    
    
    public long getCreated() {
        return created;
    }
    
    
    public long getModified() {
        return modified;
    }
    
    
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
    
    
    public OutputStream setContent() throws XmldbException {
        try {
            return new InsertStream(this);
        } catch (IOException e) {
            String msg = "Could not store document: " + this;
            logger.error(msg, e);
            throw new XmldbException(msg, e);
        }
    }
    
    
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
    
    
//    public void setKey(String name, Object value) {
//        Collection col = getParent();
//        col.addIndexValue(name, value, id);
//    }
    
    
    //------------------------------------------------------------------------
    //  Interface implementation: Comparable
    //------------------------------------------------------------------------

    
    public int compareTo(Document doc) {
        //TODO: Maybe compare documents by URI (slower)?
//        return name.compareTo(doc.getUri());
        return name.compareTo(doc.getName());
    }
    

    //------------------------------------------------------------------------
    //  Overriden methods: Object
    //------------------------------------------------------------------------

    
    @Override
    public String toString() {
        return getUri();
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
