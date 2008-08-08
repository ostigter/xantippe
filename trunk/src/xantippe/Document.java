package xantippe;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

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
	private DatabaseImpl database;

	/** ID. */
	private int id;
	
	/** Name. */
	private String name;
	
	/** Media type. */
	private MediaType mediaType;
	
	/** Parent collection ID. */
	private int parent;
	
	/** Keys mapped by their name. */
	private Map<String, Key> keys;
	
	
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
		
		keys = new HashMap<String, Key>();
		
		database.addDocument(this);
	}
	
	
    //------------------------------------------------------------------------
    //  Public methods
    //------------------------------------------------------------------------

	
	public String getName() {
		return name;
	}
	
	
//	public void setName(String name) {
//		this.name = name;
//	}
	
	
	public Collection getParent() {
		return database.getCollection(parent);
	}
	
	
	public MediaType getMediaType() {
		return mediaType;
	}
	
	
	public Key[] getKeys() {
		return keys.values().toArray(new Key[0]);
	}


	public Object getKey(String name) {
		Key key = keys.get(name);
		return (key != null) ? key.getValue() : null;
	}
	
	
	public void setKey(String name, Object value) {
		Key key = keys.get(name);
		if (key == null) {
			key = new Key(name, value);
            keys.put(name, key);
		} else {
			key.setValue(value);
		}
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
            String msg = "Could not retrieve document: " + this;
	        logger.error(msg, e);
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
    		//TODO: Determine correct validation mode.
    		boolean required = false;
    	    database.getValidator().validate(file, required);
    	}
    	
		storeDocument(file);
	}
	
	
    //------------------------------------------------------------------------
    //  Interface implementation: Comparable
    //------------------------------------------------------------------------

	
	public int compareTo(Document doc) {
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
	
	
    //------------------------------------------------------------------------
    //  Package protected methods
    //------------------------------------------------------------------------
	
	
	private void storeDocument(File file) throws XmldbException {
        if (mediaType == MediaType.XML) {
	        database.indexDocument(this);
        }
        
	    try {
	        database.getFileStore().store(id, file);
	    } catch (FileStoreException e) {
	        String msg = "Could not store document: " + this;
	        logger.error(msg, e);
	        throw new XmldbException(msg, e);
	    }
	}
    
	
}
