package xantippe;


import java.io.File;
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
    private final DatabaseImpl database;

    /** ID. */
    private final int id;
    
    /** Child collections. */
    private final Set<Integer> collections;
    
    /** Documents. */
    private final Set<Integer> documents;
    
    /** Index definitions. */
    private final Set<Index> indices;
    
    /** Index values mapped by key name. */
    private final Map<String, IndexValue> indexValues;
    
    /** Name. */
    private String name;
    
    /** Parent collection. */
    private int parent;
    
    /** Validation mode. */
    private ValidationMode validationMode = ValidationMode.INHERIT;
    
    /** Whether documents in this collection should be compressed. */  
    private CompressionMode compressionMode = CompressionMode.INHERIT;
    
    
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
        if (parent != -1) {
            return database.getCollection(parent);
        } else {
            // Root collection has no parent.
            return null;
        }
    }
    
    
    public Set<Collection> getCollections() {
        Set<Collection> cols = new TreeSet<Collection>();
        for (int id : collections) {
            cols.add(database.getCollection(id));
        }
        return cols;
    }
    
    
    public Collection getCollection(String name) {
    	for (int colId : collections) {
    		Collection col = database.getCollection(colId);
    		if (col != null && col.getName().equals(name)) {
    		    // Found.
    		    return col;
    		}
    	}
    	
    	// Not found.
    	return null;
    }
    
    
    public Set<Document> getDocuments() {
        Set<Document> docs = new TreeSet<Document>();
        for (int id : documents) {
            Document doc = database.getDocument(id); 
            if (doc != null) {
                docs.add(doc);
            } else {
                logger.error(String.format(
                        "Document with ID %d not found", id));
            }
        }
        return docs;
    }
    
    
    public Document getDocument(String name) {
        for (int docId : documents) {
            Document doc = database.getDocument(docId);
            if (doc != null && doc.getName().equals(name)) {
                // Found.
                return doc;
            }
        }
        
        // Not found.
        return null;
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
    
    
    public void setValidationMode(ValidationMode validationMode) {
        if (parent == -1 && validationMode == ValidationMode.INHERIT) {
            logger.error("Invalid validation mode for root collection");
        } else {
            this.validationMode = validationMode;
        }
    }
    
    
    public CompressionMode getCompressionMode(boolean inherited) {
        CompressionMode cm = compressionMode;
        
        if (inherited && cm == CompressionMode.INHERIT) {
            Collection col = getParent();
            if (col != null) {
                cm = col.getCompressionMode(true);
            } else {
                // Root collection cannot inherit! 
                logger.error("Invalid compression mode for root collection");
                cm = CompressionMode.NONE;
            }
        }
        
        return cm;
    }
    
    
    public void setCompressionMode(CompressionMode compressionMode) {
        if (parent == -1 && compressionMode == CompressionMode.INHERIT) {
            logger.error("Invalid compression mode for root collection");
        } else {
            this.compressionMode = compressionMode;
        }
    }
    

    public Set<Index> getIndices(boolean inherited) {
        Set<Index> indices2 = new TreeSet<Index>();
        
        // Add indices from this collection.
        indices2.addAll(indices);
        
        if (inherited) {
            // Add inherited indices from parent collections.
            Collection col = getParent();
            if (col != null) {
                indices2.addAll(col.getIndices(inherited));
            }
        }
        
        return indices2;
    }
    
    
    public Index getIndex(String name) {
        Index index = null;
        Set<Index> inheritedIndices = getIndices(true);
        for (Index i : inheritedIndices) {
            if (i.getName().equals(name)) {
                index = i;
                break;
            }
        }
        return index;
    }
    
    
    public void addIndex(String name, String path, IndexType type) {
        Index index = getIndex(name);
        if (index == null) {
            int newId = database.getNextId();
            indices.add(new Index(newId, name, path, type));
            logger.debug(String.format(
                    "Added index '%s' for collection '%s'", name, this));
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
    
    
    public Document createDocument(String name) throws XmldbException {
        return createDocument(name, database.getMediaType(name));
    }
    
    
    public Document createDocument(String name, MediaType mediaType)
            throws XmldbException {
        if (name == null) {
            throw new IllegalArgumentException("Null name");
        }
        
        Document doc = getDocument(name);
        if (doc == null) {
            int docId = database.getNextId();
            doc = new Document(database, docId, name, mediaType, id);
            documents.add(docId);
            logger.debug(String.format("Created document '%s'", doc));
        } else {
            String msg = String.format("Document already exists: '%s'", doc);
            throw new XmldbException(msg);
        }
        
        return doc;
    }
    
    
    public Collection createCollection(String name) throws XmldbException {
    	if (name == null || name.length() == 0) {
    		throw new IllegalArgumentException("Null or empty name");
    	}
    	
    	Collection col = getCollection(name);
    	if (col == null) {
	        int colId = database.getNextId(); 
	        col = new Collection(database, colId, name, id);
	        collections.add(colId);
	        logger.debug(String.format("Created collection '%s'", this));
    	} else {
    		String msg = String.format("Collection already exists: '%s'", col);
    		throw new XmldbException(msg);
    	}
    	
        return col;
    }
    
    
    public boolean deleteDocument(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Null name");
        }
        
        boolean deleted = false;
        Document doc = getDocument(name);
        if (doc != null) {
            deleteDocument(doc);
            deleted = true;
        }
        return deleted;
    }
    
    
    public boolean deleteCollection(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Null name");
        }
        
        boolean deleted = false;
        Collection col = getCollection(name);
        if (col != null) {
            col.delete();
            collections.remove(col.getId());
            deleted = true;
        }
        return deleted;
    }
    
    
    public Set<Document> findDocuments(Key[] keys, boolean recursive)
            throws XmldbException {
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
        
        Set<Document> docs = new TreeSet<Document>();
        
        findDocuments(keys, recursive, docs);
        
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
    
    
    /* package */ ValidationMode getExplicitValidationMode() {
        return validationMode;
        
    }
    
    
    /* package */ void addIndex(Index index) {
        indices.add(index);
    }


    /* package */ void addDocument(int id) {
        documents.add(id);
    }


    /* package */ void addCollection(int id) {
        collections.add(id);
    }


    /* package */ void indexDocument(Document doc, File file) {
        database.getIndexer().index(doc, file, this);
        
    }
    
    
    /* package */ void addIndexValue(
            String keyName, Object keyValue, int docId) {
        // Check that document really is in this collection.
        if (documents.contains(docId)) {
            IndexValue iv = indexValues.get(keyName);
            if (iv == null) {
                iv = new IndexValue();
                indexValues.put(keyName, iv);
            }
            iv.indexDocument(docId, keyValue);
        } else {
            // For debugging only.
            logger.warn("Indexed document not found in collection");
        }
    }
    
    
    /* package */ Map<String, IndexValue> getIndexValues() {
        return indexValues;
    }
    
    
    /* package */ void delete() {
        for (Document doc : getDocuments()) {
            deleteDocument(doc);
        }
        for (Collection col : getCollections()) {
            collections.remove(col.getId());
            col.delete();
        }
        indexValues.clear();
        indices.clear();
        database.deleteCollection(this);
        String msg = String.format("Deleted collection '%s'", this);
        logger.debug(msg);
    }


    //------------------------------------------------------------------------
    //  Private methods
    //------------------------------------------------------------------------
    
    
    // FIXME: Get rid of "unchecked" warning
    @SuppressWarnings("unchecked")  // new HashSet[]
    private void findDocuments(
            Key[] keys, boolean recursive, Set<Document> docs) {
        int noOfKeys = keys.length;
        
        // Find documents (ID's) that match any key, sorted per key.
        Set<Integer>[] docsPerKey = new HashSet[noOfKeys];
        for (int i = 0; i < noOfKeys; i++) {
            docsPerKey[i] = new HashSet<Integer>();
            IndexValue value = indexValues.get(keys[i].getName());
            if (value != null) {
                docsPerKey[i].addAll(value.findDocuments(keys[i].getValue()));
            }
        }
        
        // Gather documents by ID's.
        if (noOfKeys == 1) {
            for (int id : docsPerKey[0]) {
                // Check document to be really in this collection.
                if (documents.contains(id)) {
                    docs.add(database.getDocument(id));
                }
            }
        } else {
            // Only keep documents that match all keys.
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
            
            // Add matching documents to final set.
            for (int id : matchingIds) {
                // Check document to be really in this collection.
                if (documents.contains(id)) {
                    docs.add(database.getDocument(id));
                }
            }
        }
        
        // Optionally search subcollections.
        if (recursive) {
            for (Collection col : getCollections()) {
                col.findDocuments(keys, recursive, docs);
            }
        }
    }
    
    
    private void deleteDocument(Document doc) {
        documents.remove(doc.getId());
        doc.delete();
    }

    
}
