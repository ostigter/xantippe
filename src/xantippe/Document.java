package xantippe;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
    
    /** Back-reference to the database. */
    private final DatabaseImpl database;

    /** ID. */
    private final int id;
    
    /** Name. */
    private String name;
    
    /** Media type. */
    private MediaType mediaType;
    
    /** Parent collection ID. */
    private int parent;
    
    
    //------------------------------------------------------------------------
    //  Constructors
    //------------------------------------------------------------------------

    
    /* package */ Document(DatabaseImpl database, int id, String name,
            MediaType mediaType, int parent) {
        this.database = database;
        this.id = id;
        this.name = name;
        this.mediaType = mediaType;
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
            throw new IllegalArgumentException("file is null");
        }
        
        if (!file.exists()) {
            String msg = String.format("File not found: '%s'", file);
            logger.error(msg);
            throw new XmldbException(msg);
        }
        
        if (!file.isFile()) {
            String msg = String.format("Not a file: '%s'", file);
            logger.error(msg);
            throw new XmldbException(msg);
        }
        
        if (!file.canRead()) {
            String msg =
                    String.format("No permission to read file: '%s'", file);
            logger.error(msg);
            throw new XmldbException(msg);
        }
        
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
            ValidationMode vm = getParent().getValidationMode();
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
                    logger.error("Invalid validation mode: " + vm);
            }
        }
        
        try {
            database.getFileStore().store(id, file);
            
            if (mediaType == MediaType.XML) {
            	getParent().indexDocument(this, file);
            }
        } catch (FileStoreException e) {
            String msg = "Could not store document: " + this;
            logger.error(msg, e);
            throw new XmldbException(msg, e);
        }
    }
    
    
    public void setKey(String name, Object value) {
        Collection col = getParent();
        col.addIndexValue(name, value, id);
    }
    
    
    //------------------------------------------------------------------------
    //  Interface implementation: Comparable
    //------------------------------------------------------------------------

    
    public int compareTo(Document doc) {
        //FIXME: Maybe compare documents by URI (slower)?
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
    
    
    /* package */ void delete() {
        database.deleteDocument(this);
        String msg = String.format("Deleted document '%s'", this);
        logger.debug(msg);
    }
    
    
}
