package xantippe;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;


/**
 * Default implementation of the database.
 * 
 * @author  Oscar Stigter
 */
public class DatabaseImpl implements Database {
	
	
	/** log4j logger. */
	private static final Logger logger = Logger.getLogger(DatabaseImpl.class);
	
    /** Database directory. */
    private String dataDir = System.getProperty("xantippe.data.dir", "data");
    
    /** Whether the database is running. */
    private boolean isRunning = false;

	/** Root collection. */
	private Collection rootCollection;
	
	/** Collections mapped by ID. */
	private Map<Integer, Collection> collections;
    
	/** Documents mapped by ID. */
	private Map<Integer, Document> documents;
	
    /** Indexes mapped by key name. */
    private Map<String, IndexValue> indexes;
    
    /** Next document ID. */
    private int nextId;
    
	
    //------------------------------------------------------------------------
    //  Constructors
    //------------------------------------------------------------------------


    /**
     * Zero-argument constructor.
     */
    public DatabaseImpl() {
    	Util.initLog4j();
    	
    	collections = new HashMap<Integer, Collection>();
    	documents = new HashMap<Integer, Document>();
        indexes = new HashMap<String, IndexValue>();
        
        logger.debug("Data directory: " + dataDir);
		
		logger.debug("Database created.");
	}
    
    
    //------------------------------------------------------------------------
    //  Interface implementation: Database
    //------------------------------------------------------------------------
    

    public void start() throws XmldbException {
        if (isRunning) {
            throw new XmldbException("Database already running");
        }
        
		logger.debug("Starting database.");
		
        // Create database directory if necessary. 
        File dir = new File(dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        
        readMetaData();
        
        readCollections();
        
        isRunning = true;

		logger.debug("Database started.");
    }
    
    
    public void shutdown() throws XmldbException {
        checkRunning();
        
		logger.debug("Shutting down database.");
		
        writeMetaData();
        
        writeCollections();
        
        isRunning = false;
        
		logger.debug("Database shut down.");
    }
    
    
    public boolean isRunning() {
        return isRunning;
    }
    
    
    public Collection getRootCollection() throws XmldbException {
        checkRunning();
    	return rootCollection;
    }
    
    
	@SuppressWarnings("unchecked")  // new HashSet[]
    public Set<Document> findDocuments(Key[] keys) throws XmldbException {
	    checkRunning();
	    
	    if (keys == null) {
	    	String msg = "findDocuments(): Null value for keys";
	    	logger.error(msg);
	    	throw new IllegalArgumentException(msg);
	    }
	    
        int noOfKeys = keys.length;
	    if (noOfKeys == 0) {
	    	String msg = "findDocuments(): Empty key array";
	    	logger.error(msg);
	    	throw new IllegalArgumentException(msg);
	    }
        
        Set<Integer>[] docsPerKey = new HashSet[noOfKeys];

        // Find documents (ID's) that match any key.
        for (int i = 0; i < noOfKeys; i++) {
            Key key = keys[i];
            docsPerKey[i] = new HashSet<Integer>();
            IndexValue index = indexes.get(key.getName());
            if (index != null) {
                docsPerKey[i].addAll(index.findDocuments(key.getValue()));
            }
        }
        
        // Gather documents by ID's.
        Set<Document> docs = new HashSet<Document>();
        if (noOfKeys == 1) {
            for (int id : docsPerKey[0]) {
                docs.add(documents.get(id));
            }
        } else {
            // Filter out documents that do not match ALL keys.
            Set<Integer> matchingIds = new HashSet<Integer>();
            for (int id : docsPerKey[0]) {
                boolean matches = true;
                for (int i = 1; i < noOfKeys; i++) {
                    if (!docsPerKey[i].contains(id)) {
                        matches = false;
                        break;
                    }
                }
                if (matches) {
                    matchingIds.add(id);
                }
            }
            
            for (int id : matchingIds) {
                docs.add(documents.get(id));
            }
        }
	    
	    return docs;
	}
	
	
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
	
	
    /* package */ void indexDocument(Document doc) {
        for (Key key : doc.getKeys()) {
            String keyName = key.getName();
            IndexValue index = indexes.get(keyName);
            if (index == null) {
                index = new IndexValue();
                indexes.put(keyName, index);
            }
            index.indexDocument(doc, key.getValue());
        }
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
        File file = new File(dataDir + "/metadata.dbx");
    	if (file.exists()) {
    		try {
    			DataInputStream dis =
    					new DataInputStream(new FileInputStream(file));
    			nextId = dis.readInt();
    			dis.close();
    		} catch (IOException e) {
                logger.error("ERROR: Could not read metadata.dbx", e);
    		}
    	} else {
    		nextId = 1;
    	}
    }
    
    
    private void writeMetaData() {
        File file = new File(dataDir + "/metadata.dbx");
		try {
			DataOutputStream dos =
					new DataOutputStream(new FileOutputStream(file));
			dos.writeInt(nextId);
			dos.close();
		} catch (IOException e) {
			System.err.println(e);
		}
    }
    
    
    private void readCollections() {
        File file = new File(dataDir + "/collections.dbx");
    	if (file.exists()) {
    		try {
    			DataInputStream dis = new DataInputStream(
    					new FileInputStream(file));
    			rootCollection = readCollection(dis, -1);
    			dis.close();
    		} catch (IOException e) {
    			logger.error("ERROR: Could not read collections.dbx", e);
    		}
    	} else {
    	    int id = getNextId();
            rootCollection = new Collection(this, id, "db", -1);
    	}
    }
    
    
    private Collection readCollection(DataInputStream dis, int parent)
            throws IOException {
        Collection col;
        
        int id = dis.readInt();
        String name = dis.readUTF();
        col = new Collection(this, id, name, parent);
        int noOfDocs = dis.readInt();
        for (int i = 0; i < noOfDocs; i++) {
            int docId = dis.readInt();
            String docName = dis.readUTF();
            Document doc = new Document(this, docId, docName, id);
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
        File file = new File(dataDir + "/collections.dbx");
		try {
			DataOutputStream dos = new DataOutputStream(
					new FileOutputStream(file));
			writeCollection(rootCollection, dos);
			dos.close();
		} catch (IOException e) {
			logger.error("Could not write collections.dbx", e);
		}
    }
    
    
    private void writeCollection(Collection col, DataOutputStream dos)
    		throws IOException {
    	dos.writeInt(col.getId());
    	dos.writeUTF(col.getName());
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
    }
    
    
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
