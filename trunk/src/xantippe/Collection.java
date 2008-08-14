package xantippe;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;


/**
 * A collection of documents and subcollections.
 * 
 * @author Oscar Stigter
 */
public class Collection implements Comparable<Collection> {
	
	
	/** log4j logger. */
	private static final Logger logger = Logger.getLogger(Collection.class);
	
	/** Back-reference to the database. */
	private DatabaseImpl database;

	/** ID. */
	private int id;
	
	/** Name. */
	private String name;
	
	/** Parent collection. */
	private int parent;
	
	/** Child collections. */
	private Set<Integer> collections;
	
	/** Documents. */
	private Set<Integer> documents;
	
	/** Validation mode. */
	private ValidationMode validationMode = ValidationMode.INHERIT;
	
	/** Index definitions. */
	private Set<Index> indices;
	
    /** Index values mapped by key name. */
    private Map<String, IndexValue> indexValues;
    
	
    //------------------------------------------------------------------------
    //  Constructors
    //------------------------------------------------------------------------

	
	/* package */ Collection(
			DatabaseImpl database, int id, String name, int parent) {
	    this.id = id;
		this.database = database;
		this.name = name;
		this.parent = parent;
		
		collections = new TreeSet<Integer>();
		documents = new TreeSet<Integer>();
		indices = new TreeSet<Index>();
        indexValues = new HashMap<String, IndexValue>();
		
		database.addCollection(this);
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
	
	
	public Set<Collection> getCollections() {
	    Set<Collection> cols = new TreeSet<Collection>();
	    for (int id : collections) {
	        cols.add(database.getCollection(id));
	    }
		return cols;
	}
	
	
	public ValidationMode getValidationMode() {
		ValidationMode vm = validationMode;
		if (vm == ValidationMode.INHERIT) { 
			Collection col = getParent();
			if (col != null) {
				vm = col.getValidationMode();
			} else {
				// Root collection cannot inherit! 
				logger.error("Invalid validation mode on root collection");
				vm = ValidationMode.OFF;
			}
		}
		return vm;
	}
	
	
	public void setValidationMode(ValidationMode vm) {
		if (parent == -1 && vm == ValidationMode.INHERIT) {
			logger.debug("Invalid validation mode on root collection");
		} else {
			validationMode = vm;
		}
	}
	

	public Set<Document> getDocuments() {
        Set<Document> docs = new TreeSet<Document>();
        for (int id : documents) {
            docs.add(database.getDocument(id));
        }
        return docs;
	}
	
	
	public Set<Index> getIndices() {
		Set<Index> indices2 = new TreeSet<Index>();
		
		// Add indices from this collection.
		indices2.addAll(indices);
		
		// Add inherited indices from parent collections.
		Collection col = getParent();
		if (col != null) {
			indices2.addAll(col.getIndices());
		}
		
		return indices2;
	}
	
	
	public Index getIndex(String name) {
		Index index = null;
		Set<Index> inheritedIndices = getIndices();
		for (Index i : inheritedIndices) {
			if (i.getName().equals(name)) {
				index = i;
				break;
			}
		}
		return index;
	}
	
	
	public void addIndex(String name, String path) {
		Index index = getIndex(name);
		if (index == null) {
			int newId = database.getNextId();
			indices.add(new Index(newId, name, path));
		} else {
			String msg = "Index name already used (possibly inherited)";
			throw new IllegalArgumentException(msg);
		}
	}
	
	
	public String getUri() {
		StringBuilder sb = new StringBuilder();
		Collection col = this;
		while (col != null) {
			sb.insert(0, '/'); 
			sb.insert(1, col.getName());
			col = col.getParent();
		}
		return sb.toString();
	}
	
	
	public Collection createCollection(String name) {
	    int colId = database.getNextId(); 
		Collection col = new Collection(database, colId, name, id);
		collections.add(colId);
		return col;
	}
	
	
	public Document createDocument(String name) {
		return createDocument(name, database.getMediaType(name));
	}
	
	
	public Document createDocument(String name, MediaType mediaType) {
	    int docId = database.getNextId();
		Document doc = new Document(database, docId, name, mediaType, id);
		documents.add(docId);
		return doc;
	}
	
	
	@SuppressWarnings("unchecked")  // new HashSet[]
    public Set<Document> findDocuments(Key[] keys) throws XmldbException {
	    if (keys == null) {
	    	String msg = "Null key array";
	    	logger.error(msg);
	    	throw new IllegalArgumentException(msg);
	    }
	    
        int noOfKeys = keys.length;
	    if (noOfKeys == 0) {
	    	String msg = "Empty key array";
	    	logger.error(msg);
	    	throw new IllegalArgumentException(msg);
	    }
        
        Set<Integer>[] docsPerKey = new HashSet[noOfKeys];
        
        //TODO: Recurse into collections.

        // Find documents (ID's) that match any key.
        for (int i = 0; i < noOfKeys; i++) {
            Key key = keys[i];
            docsPerKey[i] = new HashSet<Integer>();
            IndexValue value = indexValues.get(key.getName());
            if (value != null) {
                docsPerKey[i].addAll(value.findDocuments(key.getValue()));
            }
        }
        
        // Gather documents by ID's.
        Set<Document> docs = new HashSet<Document>();
        if (noOfKeys == 1) {
            for (int id : docsPerKey[0]) {
                docs.add(database.getDocument(id));
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
                docs.add(database.getDocument(id));
            }
        }
	    
	    return docs;
	}
	
	
    //------------------------------------------------------------------------
    //  Interface implementation: Comparable
    //------------------------------------------------------------------------

	
	public int compareTo(Collection col) {
		return name.compareTo(col.getName());
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
	
	
	/* package */ void addDocument(int id) {
	    documents.add(id);
	}


    /* package */ void addCollection(int id) {
        collections.add(id);
    }


    /* package */ void indexDocument(Document doc) {
        for (Key key : doc.getKeys()) {
            String keyName = key.getName();
            IndexValue value = indexValues.get(keyName);
            if (value == null) {
                value = new IndexValue();
                indexValues.put(keyName, value);
            }
            value.indexDocument(doc, key.getValue());
        }
    }


}
