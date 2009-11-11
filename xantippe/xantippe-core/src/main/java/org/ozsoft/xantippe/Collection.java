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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A collection containing documents and/or subcollections.
 * 
 * @author Oscar Stigter
 */
public class Collection implements Comparable<Collection> {
    
    /** Log */
    private static final Log LOG = LogFactory.getLog(Collection.class);
    
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
    
    /** Creation timestamp. */
    private final long created;
    
    /** Modification timestamp. */
    private long modified;
    
    /** Parent collection. */
    private final int parent;
    
    /** Validation mode. */
    private ValidationMode validationMode = ValidationMode.INHERIT;
    
    /** Whether documents in this collection should be compressed. */  
    private CompressionMode compressionMode = CompressionMode.INHERIT;
    
    /* package */ Collection(
            DatabaseImpl database, int id, String name, long created,
            long modified, int parent) {
        this.id = id;
        this.database = database;
        this.name = name;
        this.created = created;
        this.modified = modified;
        this.parent = parent;
        
        collections = new TreeSet<Integer>();
        documents = new TreeSet<Integer>();
        indices = new TreeSet<Index>();
        indexValues = new HashMap<String, IndexValue>();
        
        database.addCollection(this);
    }
    
    /**
     * Returns the name.
     * 
     * @return  the name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Returns the timestamp the collection was created.
     * 
     * @return  the timestamp in milliseconds after 01-Jan-1970.
     */
    public long getCreated() {
        return created;
    }
    
    /**
     * Returns the timestamp the collection was last modified.
     * 
     * @return  the timestamp in milliseconds after 01-Jan-1970.
     */
    public long getModified() {
        return modified;
    }
    
    /**
     * Returns the parent collection.
     * 
     * @return  the parent collection
     */
    public Collection getParent() {
        if (parent != -1) {
            return database.getCollection(parent);
        } else {
            // Root collection has no parent.
            return null;
        }
    }
    
    /**
     * Returns the child collections.
     * 
     * @return  a set with the child collections
     */
    public Set<Collection> getCollections() {
        Set<Collection> cols = new TreeSet<Collection>();
        for (int id : collections) {
            cols.add(database.getCollection(id));
        }
        return cols;
    }
    
    /**
     * Returns the child collection with the specified name, or null if no such
     * collection exists.
     * 
     * @param name  the child collection's name
     * 
     * @return  the child collection, or null if it does not exist
     */
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
    
    /**
     * Returns the documents in this collection.
     * 
     * @return  a set with the documents
     */
    public Set<Document> getDocuments() {
        Set<Document> docs = new TreeSet<Document>();
        for (int id : documents) {
            Document doc = database.getDocument(id); 
            if (doc != null) {
                docs.add(doc);
            } else {
                LOG.error(String.format(
                        "Document with ID %d not found", id));
            }
        }
        return docs;
    }
    
    /**
     * Returns the document with the specified name, or null it such a document
     * does not exist.
     * 
     * @param name  the document's name
     * 
     * @return  the document, or null if it does not exist
     */
    public Document getDocument(String name) {
        database.getLockManager().lockRead(this);
        try {
            for (int docId : documents) {
                Document doc = database.getDocument(docId);
                if (doc != null && doc.getName().equals(name)) {
                    // Found.
                    return doc;
                }
            }
            // Not found.
            return null;
        } finally {
            database.getLockManager().unlockRead(this);
        }
    }
    
    /**
     * Returns the validation mode for this collection.
     * 
     * @param  inherit  in case the validation mode is to inherit from the
     *                  parent collection, whether to return the inherited mode
     *                  (true), or the actual mode of this collection (false)
     * 
     * @return  the validation mode
     */
    public ValidationMode getValidationMode(boolean inherit) {
        ValidationMode vm = validationMode;
        if (inherit && vm == ValidationMode.INHERIT) { 
            vm = getParent().getValidationMode(true);
        }
        return vm;
    }
    
    /**
     * Sets the validation mode.
     * 
     * Setting the INHERIT mode on the root collection is invalid and will be
     * ignored.
     * 
     * @param  validationMode  the validation mode
     */
    public void setValidationMode(ValidationMode validationMode) {
        if (parent == -1 && validationMode == ValidationMode.INHERIT) {
            LOG.error("Invalid validation mode for root collection");
        } else {
            this.validationMode = validationMode;
            updateModified();
        }
    }
    
    /**
     * Returns the compression mode for this collection.
     * 
     * @param  inherited  in case the compression mode is to inherit from the
     *                    parent collection, whether to return the inherited
     *                    mode (true), or the actual mode of this collection
     *                    (false)
     * 
     * @return  the compression mode
     */
    public CompressionMode getCompressionMode(boolean inherited) {
        CompressionMode cm = compressionMode;
        if (inherited && cm == CompressionMode.INHERIT) {
            cm = getParent().getCompressionMode(true);
        }
        return cm;
    }
    
    /**
     * Sets the compression mode.
     * 
     * Setting the INHERIT mode on the root collection is invalid and will be
     * ignored.
     * 
     * @param  compressionMode  the compression mode
     */
    public void setCompressionMode(CompressionMode compressionMode) {
        if (parent == -1 && compressionMode == CompressionMode.INHERIT) {
            LOG.error("Invalid compression mode for root collection");
        } else {
            this.compressionMode = compressionMode;
            updateModified();
        }
    }
    
    /**
     * Returns the index definitions configured for this collection.
     * 
     * @param  inherited  whether to include the indices inherited from parent
     *                    collections
     *                    
     * @return  the index definitions
     */
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
    
    /**
     * Returns the index definition with the specified name, or null if not
     * found.
     * 
     * @param name  the index name
     * 
     * @return  the index definition, or null if not found
     */
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
    
    /**
     * Adds an index definition.
     * 
     * @param   name  the index name
     * @param   path  the index path
     * @param   type  the index type
     * 
     * @throws  IllegalArgumentException
     *              if an index with the specified name already exists
     *              (possibly inherited)
     */
    public void addIndex(String name, String path, IndexType type) {
        Index index = getIndex(name);
        if (index == null) {
            int newId = database.getNextId();
            indices.add(new Index(newId, name, path, type));
            updateModified();
            LOG.debug(String.format(
                    "Added index '%s' for collection '%s'", name, this));
        } else {
            String msg = "Index name already used (possibly inherited)";
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * Returns the absolute URI.
     * 
     * @return  the URI
     */
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
    
    /**
     * Creates a document with the specified name.
     * 
     * The document's media type will be automatically determined based on its
     * extention.
     * 
     * @param   name  the document name
     * 
     * @return  the created document
     * 
     * @throws  XmldbException
     *             if the document could not be created, or a document with the
     *             same name already exists
     */
    public Document createDocument(String name) throws XmldbException {
        return createDocument(name, database.getMediaType(name));
    }
    
    /**
     * Creates a document with the specified name and media type.
     * 
     * @param  name       the name
     * @param  mediaType  the media type
     * 
     * @return  the created document
     * 
     * @throws  IllegalArgumentException
     *              if the name is null or empty
     * @throws  XmldbException
     *              if the document could not be created, or a document with
     *              the same name already exists
     */
    public Document createDocument(String name, MediaType mediaType)
            throws XmldbException {
        if (name == null) {
            throw new IllegalArgumentException("Null name");
        }
        
        Document doc = getDocument(name);
        if (doc != null) {
            String msg = String.format("Document already exists: '%s'", doc);
            throw new XmldbException(msg);
        }
        
        database.getLockManager().lockWrite(this);
        try { 
            int docId = database.getNextId();
            long timestamp = System.currentTimeMillis();
            doc = new Document(
                    database, docId, name, mediaType, timestamp, timestamp, id);
            documents.add(docId);
            updateModified();
        } finally {
            database.getLockManager().unlockWrite(this);
        }
        
        LOG.debug(String.format("Created document '%s'", doc));
        
        return doc;
    }
    
    /**
     * Creates a child collection with the specified name.
     * 
     * @param name  the collection's name
     * 
     * @return  the created collection
     * 
     * @throws  IllegalArgumentException
     *              if the name is null or empty
     * @throws  XmldbException
     *              if the collection could not be created, or a child
     *              collection with the specified name already exists
     */
    public Collection createCollection(String name) throws XmldbException {
    	if (name == null || name.length() == 0) {
    		throw new IllegalArgumentException("Null or empty name");
    	}
    	
    	Collection col = getCollection(name);
    	if (col == null) {
    	    database.getLockManager().lockWrite(this);
    	    try {
    	        int colId = database.getNextId();
    	        long timestamp = System.currentTimeMillis();
    	        col = new Collection(database, colId, name, timestamp, timestamp, id);
    	        collections.add(colId);
                updateModified();
    	        LOG.debug(String.format("Created collection '%s'", this));
    	    } finally {
                database.getLockManager().unlockWrite(this);
    	    }
    	} else {
    		String msg = String.format("Collection already exists: '%s'", col);
    		throw new XmldbException(msg);
    	}
    	
        return col;
    }
    
    /**
     * Deletes the document with the specified name.
     * 
     * @param name  the document's name
     * 
     * @return  true if the document has been deleted, otherwise false
     * 
     * @throws  IllegalArgumentException  if the name is null or empty
     */
    public boolean deleteDocument(String name) {
        if (name == null || name.length() == 0) {
            throw new IllegalArgumentException("Null or empty name");
        }
        
        database.getLockManager().lockWrite(this);
        try {
            boolean deleted = false;
            Document doc = getDocument(name);
            if (doc != null) {
                deleteDocument(doc);
                deleted = true;
            }
            return deleted;
        } finally {
            database.getLockManager().unlockWrite(this);
        }
    }
    
    /**
     * Deletes the child collection with the specified name.
     * 
     * Any child documents and collections in the specified collection will be
     * deleted, too.
     *  
     * @param name  the collection's name
     * 
     * @return  true if the collection was deleted, otherwise false
     * 
     * @throws  IllegalArgumentException
     *              if the collection name is null or empty
     */
    public boolean deleteCollection(String name) {
        if (name == null || name.length() == 0) {
            throw new IllegalArgumentException("Null or empty name");
        }
        
        database.getLockManager().lockWrite(this);
        try {
            boolean deleted = false;
            Collection col = getCollection(name);
            if (col != null) {
                col.delete();
                collections.remove(col.getId());
                updateModified();
                deleted = true;
            }
            return deleted;
        } finally {
            database.getLockManager().unlockWrite(this);
        }
    }
    
    /**
     * Returns the indiced documents in this collection and (optionally) child
     * collections that match the specified keys.
     * 
     * @param  keys       array of one or more Key objects 
     * @param  recursive  whether to search child collections, too
     * 
     * @return  a set of found documents
     * 
     * @throws  IllegalArgumentException  if the keys array is null or empty
     */
    public Set<Document> findDocuments(IndexKey[] keys, boolean recursive) {
        if (keys == null || keys.length == 0) {
            throw new IllegalArgumentException("Null or empty key array");
        }
        
        Set<Document> docs = new TreeSet<Document>();
        findDocuments(keys, recursive, docs);
        return docs;
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
     * Returns true if this collection is equal to the specified Object,
     * otherwise false
     * 
     * @param  obj  the Object to compare this collection to
     */
    @Override // Object
    public boolean equals(Object obj) {
        if (obj instanceof Collection) {
            Collection col = (Collection) obj;
            return col.getId() == id;
            
        } else {
            return false;
        }
    }
    
    /**
     * Compares this collection with another collection.
     * 
     * Collections are compared based on their name (alphabetically).
     * 
     * @param  col  the collect to compares this collection with
     * 
     * @return  the comparison value 
     */
    public int compareTo(Collection col) {
        return name.compareTo(col.getName());
    }
    
    /**
     * Returns a string representation of this collection.
     * 
     * A collection is represented by its absolute URI.
     * 
     * @return  a string representation of this collection
     */
    @Override
    public String toString() {
        return getUri();
    }
    
    /* package */ int getId() {
        return id;
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
        // Make sure document really is in this collection.
        if (documents.contains(docId)) {
            IndexValue iv = indexValues.get(keyName);
            if (iv == null) {
                iv = new IndexValue();
                indexValues.put(keyName, iv);
            }
            iv.indexDocument(docId, keyValue);
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
        
        LOG.debug(String.format("Deleted collection '%s'", this));
    }

    // FIXME: Get rid of "unchecked" warning (Eclipse bug?)
    @SuppressWarnings("unchecked")  // new HashSet[]
    private void findDocuments(IndexKey[] keys, boolean recursive, Set<Document> docs) {
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
                // Make sure document really is in this collection.
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
                // Make sure document really is in this collection.
                if (documents.contains(id)) {
                    docs.add(database.getDocument(id));
                }
            }
        }
        
        // Optionally search in subcollections.
        if (recursive) {
            for (Collection col : getCollections()) {
                col.findDocuments(keys, recursive, docs);
            }
        }
    }
    
    private void deleteDocument(Document doc) {
        database.getLockManager().lockWrite(doc);
        try {
            documents.remove(doc.getId());
            if (doc.getMediaType() == MediaType.SCHEMA) {
                // Unregister schema.
            }
            
            updateModified();
            doc.delete();
        } finally {
            database.getLockManager().unlockWrite(doc);
        }
    }
    
    private void updateModified() {
        modified = System.currentTimeMillis();
    }

}
