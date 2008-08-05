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
	
	/** Parent collection ID. */
	private int parent;
	
	/** Keys mapped by their name. */
	private Map<String, Key> keys;
	
	
    //------------------------------------------------------------------------
    //  Constructors
    //------------------------------------------------------------------------

	
	/* package */ Document(
	        DatabaseImpl database, int id, String name, int parent) {
	    this.database = database;
		this.id = id;
		this.name = name;
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
	
	
	public void setName(String name) {
		this.name = name;
	}
	
	
	public Collection getParent() {
		return database.getCollection(parent);
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
	    try {
	        database.getFileStore().store(id, file);
	        database.indexDocument(this);
	    } catch (FileStoreException e) {
	        String msg = "Could not store document: " + this;
	        logger.error(msg, e);
	        throw new XmldbException(msg, e);
	    }
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
	
	
}
