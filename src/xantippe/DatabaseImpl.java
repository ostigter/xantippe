package xantippe;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import xantippe.filestore.FileStore;
import xantippe.filestore.FileStoreException;


/**
 * Default implementation of the database.
 * 
 * @author  Oscar Stigter
 */
public class DatabaseImpl implements Database {
	
	
	/** Property for the database directory. */
	private static final String DATA_DIR = "xantippe.data.dir";
	
	/** Database file containing general metadata. */
	private static final String METADATA_FILE = "metadata.dbx";
	
	/** Database file containing the collections and documents tree. */
	private static final String COLLECTIONS_FILE = "collections.dbx";
	
	/** log4j logger. */
	private static final Logger logger = Logger.getLogger(DatabaseImpl.class);
	
    /** Database directory. */
    private final String dataDir;
    
    /** File store with the documents. */
    private final FileStore fileStore;
    
    /** Document validator. */
    private final DocumentValidator validator;

	/** Collections mapped by ID. */
	private final Map<Integer, Collection> collections;
    
	/** Documents mapped by ID. */
	private final Map<Integer, Document> documents;
	
	/** Root collection. */
	private Collection rootCollection;
	
    /** Next document ID. */
    private int nextId;
    
    /** Whether the database is running. */
    private boolean isRunning = false;
    
	
    //------------------------------------------------------------------------
    //  Constructors
    //------------------------------------------------------------------------


    /**
     * Zero-argument constructor.
     */
    public DatabaseImpl() {
    	Util.initLog4j();
    	
    	dataDir = System.getProperty("xantippe.data.dir", "data");
    	
    	fileStore = new FileStore(dataDir);
    	
    	collections = new HashMap<Integer, Collection>();
    	documents = new HashMap<Integer, Document>();
        
        validator = new DocumentValidator(this);
        
        logger.debug(String.format("Database directory: '%s'",
                new File(dataDir).getAbsolutePath()));
		
		logger.debug("Database created.");
	}
    
    
    //------------------------------------------------------------------------
    //  Interface implementation: Database
    //------------------------------------------------------------------------
    

    public void start() throws XmldbException {
        if (isRunning) {
            throw new XmldbException("Database already running");
        }
        
		logger.debug("Database starting....");
		
		try {
		    fileStore.start();
		} catch (FileStoreException e) {
		    String msg = "Could not start FileStore";
		    logger.error(msg, e);
		    throw new XmldbException(msg, e);
		}
        
        readMetaData();
        
        readCollections();
        
        validator.readSchemas(dataDir);
        
        isRunning = true;

		logger.info("Database started.");
    }
    
    
    public void shutdown() throws XmldbException {
        checkRunning();
        
		logger.debug("Database shutting down...");
		
        try {
            fileStore.shutdown();
        } catch (FileStoreException e) {
            String msg = "Could not shut down FileStore";
            logger.error(msg, e);
            throw new XmldbException(msg, e);
        }
        
        writeMetaData();
        
        writeCollections();
        
        validator.writeSchemas(dataDir);
        
        isRunning = false;
        
        logger.info("Database shut down.");
    }
    
    
    public boolean isRunning() {
        return isRunning;
    }
    
    
    public Collection getRootCollection() throws XmldbException {
        checkRunning();
    	return rootCollection;
    }
    
    
    // For debugging purposes only.
	public void print() {
	    printCollection(rootCollection);
	}
	
	
    //------------------------------------------------------------------------
    //  Package protected methods
    //------------------------------------------------------------------------
    
    
    /* package */ String getDatabaseDir() {
        return dataDir;
    }
    
    
	/* package */ int getNextId() {
		return nextId++;
	}
	
	
	/* package */ Collection getCollection(int id) {
	    return collections.get(id);
	}
	
	
    /* package */ Document getDocument(int id) {
        return documents.get(id);
    }
    
    
    /* package */ void addCollection(Collection col) {
        collections.put(col.getId(), col);
    }
    
    
	/* package */ void addDocument(Document doc) {
		documents.put(doc.getId(), doc);
	}
	
	
    /* package */ FileStore getFileStore() {
        return fileStore;
    }
    
    
    /* package */ MediaType getMediaType(String fileName) {
    	MediaType mediaType = MediaType.BINARY;
    	
    	int p = fileName.lastIndexOf('.');
    	if (p > 0) {
    		String extention = fileName.substring(p + 1).toLowerCase();
    		if (extention.equals("xml")) {
    			mediaType = MediaType.XML;
    		} else if (extention.equals("xsd")) { 
    			mediaType = MediaType.SCHEMA;
    		} else if (extention.equals("txt")) { 
    			mediaType = MediaType.PLAIN_TEXT;
    		}
    	}
    	
    	return mediaType;
    }
    
    
    /* package */ boolean isSchemaFile(String fileName) {
    	return fileName.toLowerCase().endsWith(".xsd");
    }
    
    
    /* package */ DocumentValidator getValidator() {
    	return validator;
    }
    
    
    //------------------------------------------------------------------------
    //  Private methods
    //------------------------------------------------------------------------
    
    
    private void checkRunning() throws XmldbException {
        if (!isRunning) {
            throw new XmldbException("Database not running");
        }
    }
    
    
    private void readMetaData() {
    	logger.debug(String.format("Read database file '%s'", METADATA_FILE));
        File file = new File(dataDir + '/' + METADATA_FILE);
    	if (file.exists()) {
    		try {
    			DataInputStream dis =
    					new DataInputStream(new FileInputStream(file));
    			nextId = dis.readInt();
    			dis.close();
    		} catch (IOException e) {
    			String msg = String.format(
    					"Error reading database file '%s': %s",
    					METADATA_FILE, e.getMessage());
    			logger.error(msg);
    		}
    	} else {
    		nextId = 1;
    	}
    }
    
    
    private void writeMetaData() {
    	logger.debug(String.format("Write database file '%s'", METADATA_FILE));
        File file = new File(dataDir + '/' + METADATA_FILE);
		try {
			DataOutputStream dos =
					new DataOutputStream(new FileOutputStream(file));
			dos.writeInt(nextId);
			dos.close();
		} catch (IOException e) {
			String msg = String.format(
					"Error writing database file '%s': %s",
					METADATA_FILE, e.getMessage());
			logger.error(msg);
		}
    }
    
    
    private void readCollections() {
    	logger.debug(
    			String.format("Read database file '%s'", COLLECTIONS_FILE));
        File file = new File(dataDir + '/' + COLLECTIONS_FILE);
    	if (file.exists()) {
    		try {
    			DataInputStream dis = new DataInputStream(
    					new FileInputStream(file));
    			rootCollection = readCollection(dis, -1);
    			dis.close();
    		} catch (IOException e) {
    			String msg = String.format(
    					"Error reading database file '%s': %s",
    					COLLECTIONS_FILE, e.getMessage());
    			logger.error(msg);
    		}
    	} else {
    	    int id = getNextId();
            rootCollection = new Collection(this, id, "db", -1);
            // Validation disabled by default.
            rootCollection.setValidationMode(ValidationMode.OFF);
    	}
    }
    
    
    private Collection readCollection(DataInputStream dis, int parent)
            throws IOException {
        Collection col;
        
        int id = dis.readInt();
        String name = dis.readUTF();
        col = new Collection(this, id, name, parent);
        col.setValidationMode(ValidationMode.values()[dis.readByte()]);
        
        int noOfDocs = dis.readInt();
        for (int i = 0; i < noOfDocs; i++) {
            int docId = dis.readInt();
            String docName = dis.readUTF();
            MediaType mediaType = MediaType.values()[dis.readByte()];
            Document doc = new Document(this, docId, docName, mediaType, id);
            col.addDocument(doc.getId());
        }
        int noOfCols = dis.readInt();
        for (int i = 0; i < noOfCols; i++) {
            Collection childCol = readCollection(dis, id);
            col.addCollection(childCol.getId());
        }
        
        return col;
    }
    
    
    private void writeCollections() {
    	logger.debug(
    			String.format("Write database file '%s'", COLLECTIONS_FILE));
        File file = new File(dataDir + '/' + COLLECTIONS_FILE);
		try {
			DataOutputStream dos = new DataOutputStream(
					new FileOutputStream(file));
			writeCollection(rootCollection, dos);
			dos.close();
		} catch (IOException e) {
			String msg = String.format(
					"Error writing database file '%s': %s",
					COLLECTIONS_FILE, e.getMessage());
			logger.error(msg);
		}
    }
    
    
    private void writeCollection(Collection col, DataOutputStream dos)
    		throws IOException {
    	dos.writeInt(col.getId());
    	dos.writeUTF(col.getName());
    	dos.writeByte(col.getValidationMode().ordinal());
    	Set<Document> docs = col.getDocuments();
    	dos.writeInt(docs.size());
    	for (Document doc : docs) {
    	    writeDocument(doc, dos);
    	}
    	Set<Collection> cols = col.getCollections();
    	dos.writeInt(cols.size());
    	for (Collection c : cols) {
	        writeCollection(c, dos);
    	}
    }
    
    
    private void writeDocument(Document doc, DataOutputStream dos)
            throws IOException {
        dos.writeInt(doc.getId());
        dos.writeUTF(doc.getName());
        dos.writeByte(doc.getMediaType().ordinal());
    }
    
    
    // For debugging purposes only.
    private void printCollection(Collection col) {
        System.out.println(col + "/");
        for (Document doc : col.getDocuments()) {
            System.out.println(doc);
        }
        for (Collection c : col.getCollections()) {
            printCollection(c);
        }
    }
    
    
}
