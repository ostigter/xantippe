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


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.ozsoft.xantippe.filestore.FileStore;
import org.ozsoft.xantippe.filestore.FileStoreException;



/**
 * Default implementation of the database.
 * 
 * @author  Oscar Stigter
 */
public class DatabaseImpl implements Database {
    
    
    /** Default database directory. */
    private static final String DEFAULT_DATA_DIR = "data";
    
    /** Database file containing general metadata. */
    private static final String METADATA_FILE = "metadata.dbx";

    /** Database file containing the collections and documents tree. */
    private static final String COLLECTIONS_FILE = "collections.dbx";

    /** Database file containing the indexed document key values. */
    private static final String INDICES_FILE = "indices.dbx";

    /** log4j logger. */
    private static final Logger logger = Logger.getLogger(DatabaseImpl.class);
    
    /** Collections mapped by ID. */
    private final Map<Integer, Collection> collections;
    
    /** Documents mapped by ID. */
    private final Map<Integer, Document> documents;
    
    /** Database directory. */
    private File dataDir = new File(DEFAULT_DATA_DIR);
    
    /** Root collection. */
    private Collection rootCollection;
    
    /** File store with the documents. */
    private final FileStore fileStore;
    
    /** Dataase lock manager. */
    private final LockManager lockManager;
    
    /** Document validator. */
    private final DocumentValidator validator;
    
    /** XML document indexer. */
    private final Indexer indexer;
    
    /** XQuery processor. */
    private final QueryProcessor queryProcessor;

    /** Next document ID. */
    private int nextId;
    
    /** Indicates whether the database is running. */
    private boolean isRunning = false;
    
    
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
        
        fileStore = new FileStore();
        lockManager = new LockManager();
        validator = new DocumentValidator(this);
        indexer = new Indexer();
        queryProcessor = new QueryProcessor(this);
        
        logger.debug("Database created");
    }
    
    
    //------------------------------------------------------------------------
    //  Interface implementation: Database
    //------------------------------------------------------------------------
    
    
    // See JavaDoc of Database interface
    public String getDatabaseLocation() {
        return dataDir.getAbsolutePath();
    }
    
    
    // See JavaDoc of Database interface
    public void setDatabaseLocation(String path) {
        if (isRunning) {
            throw new IllegalStateException("Database is running");
        }
        
        if (path == null || path.length() == 0) {
            throw new IllegalArgumentException(
                    "Null or empty database location");
        }
        
        dataDir = new File(path);
    }
    

    // See JavaDoc of Database interface
    public void start() throws XmldbException {
        if (isRunning) {
            throw new IllegalStateException("Database already running");
        }
        
        logger.debug("Starting database");
        
        prepareDatabaseDir();
        
        logger.debug("Database location: " + dataDir.getAbsolutePath());
        
        try {
            fileStore.setDataDir(dataDir.getAbsolutePath());
            fileStore.start();
        } catch (FileStoreException e) {
            String msg = "Could not start FileStore";
            logger.error(msg, e);
            throw new XmldbException(msg, e);
        }
        
        readMetaData();
        
        readCollections();
        
        readIndices();
        
        validator.readSchemas(dataDir);
        
        isRunning = true;

        logger.info("Database started");
    }
    
    
    // See JavaDoc of Database interface
    public void shutdown() throws XmldbException {
        checkRunning();
        
        logger.debug("Shutting down database");
        
        try {
            fileStore.shutdown();
        } catch (FileStoreException e) {
            String msg = "Could not shut down FileStore";
            logger.error(msg, e);
            throw new XmldbException(msg, e);
        }
        
        writeMetaData();
        
        writeCollections();
        
        writeIndices();
        
        validator.writeSchemas(dataDir);
        
        documents.clear();
        collections.clear();
        
        validator.clearSchemas();
        
        isRunning = false;
        
        logger.info("Database shut down");
    }
    
    
    // See JavaDoc of Database interface
    public boolean isRunning() {
        return isRunning;
    }
    
    
    // See JavaDoc of Database interface
    public Collection getRootCollection() throws XmldbException {
        checkRunning();
        return rootCollection;
    }
    
    
    // See JavaDoc of Database interface
    public Collection getCollection(String uri) throws XmldbException {
    	checkRunning();
    	
        if (uri == null || uri.length() == 0) {
            throw new IllegalArgumentException("Null or empty URI");
        }
        
    	String absoluteUri = uri;
    	
    	if (uri.startsWith("/db")) {
    		uri = uri.substring(3);
    	} else {
    		throw new XmldbException("Invalid URI: " + uri);
    	}
    	
    	Collection col = rootCollection;
    	
    	String[] parts = uri.split("/");
    	for (String colName : parts) {
    		if (colName.length() != 0) {
	    		col = col.getCollection(colName);
	    		if (col == null) {
	    			throw new XmldbException(
	    			        "Collection not found: " + absoluteUri);
	    		}
    		}
    	}
    	
    	return col;
    }
    
    
    // See JavaDoc of Database interface
    public Document getDocument(String uri) throws XmldbException {
    	checkRunning();
    	
        if (uri == null || uri.length() == 0) {
            throw new IllegalArgumentException("Null or empty URI");
        }
        
    	Document doc = null;
    	
    	int p = uri.lastIndexOf('/');
    	if (p != -1) {
    	    String colUri = uri.substring(0, p);
    	    String docName = uri.substring(p + 1);
    	    
    	    Collection col = getCollection(colUri);
    	    if (col != null) {
    	        doc = col.getDocument(docName);
    	        if (doc == null) {
    	            throw new XmldbException("Document not found: " + uri);
    	        }
    	    }
    	}
    	
    	return doc;
    }
    
    
    // See JavaDoc of Database interface
    public OutputStream executeQuery(String query) throws XmldbException {
        checkRunning();
        
        if (query == null || query.length() == 0) {
            throw new IllegalArgumentException("Null or empty query");
        }
        
    	return queryProcessor.executeQuery(query);
    }
    
    
    // See JavaDoc of Database interface
    public void print() {
        printCollection(rootCollection);
    }
    
    
    //------------------------------------------------------------------------
    //  Package protected methods
    //------------------------------------------------------------------------
    
    
    /* package */ int getNextId() {
        return nextId++;
    }
    
    
    /* package */ Collection getCollection(int id) {
    	Collection col = collections.get(id);
    	if (col == null) {
    		String msg = "Collection with ID " + id + " not found";
    		logger.error(msg);
    	}
        return col;
    }
    
    
    /* package */ boolean documentExists(int id) {
        return documents.containsKey(id);
    }
    
    
    /* package */ Document getDocument(int id) {
    	Document doc = documents.get(id);
    	if (doc == null) {
    		String msg = "Document with ID " + id + " not found";
    		logger.error(msg);
    	}
        return doc;
    }
    
    
    /* package */ void addCollection(Collection col) {
        collections.put(col.getId(), col);
    }
    
    
    /* package */ void addDocument(Document doc) {
        documents.put(doc.getId(), doc);
    }
    
    
    /* package */ void deleteCollection(Collection col) {
        collections.remove(col.getId());
    }
    
    
    /* package */ void deleteDocument(Document doc) {
        int docId = doc.getId();
        documents.remove(docId);
        try {
            fileStore.delete(docId);
        } catch (FileStoreException e) {
            String msg = String.format(
                    "Error deleting document '%s': %s", doc, e.getMessage());
            logger.error(msg, e);
        }
    }
    
    
    /* package */ FileStore getFileStore() {
        return fileStore;
    }
    
    
    /* package */ LockManager getLockManager() {
        return lockManager;
    }
    
    
    /* package */ DocumentValidator getValidator() {
        return validator;
    }
    
    
    /* package */ Indexer getIndexer() {
        return indexer;
    }
    
    
    /* package */ MediaType getMediaType(String fileName) {
        MediaType mediaType = MediaType.BINARY;
        
        //TODO: Use configurable mapping of file extentions to media types.
        
        int p = fileName.lastIndexOf('.');
        if (p > 0) {
            String extention = fileName.substring(p + 1).toLowerCase();
            if (extention.equals("xml")) {
                mediaType = MediaType.XML;
            } else if (extention.equals("xsd")) { 
                mediaType = MediaType.SCHEMA;
            } else if (extention.equals("xqy")) { 
                mediaType = MediaType.XQUERY;
            } else if (extention.equals("txt")) { 
                mediaType = MediaType.PLAIN_TEXT;
            }
        }
        
        return mediaType;
    }
    
    
    /* package */ boolean isSchemaFile(String fileName) {
        return fileName.toLowerCase().endsWith(".xsd");
    }
    
    
    //------------------------------------------------------------------------
    //  Private methods
    //------------------------------------------------------------------------
    
    
    private void checkRunning() {
        if (!isRunning) {
            throw new IllegalStateException("Database not running");
        }
    }
    
    
    private void prepareDatabaseDir() throws XmldbException {
        if (!dataDir.isDirectory()) {
            if (!dataDir.mkdirs()) {
                String msg = "Could not create database directory: " + dataDir;
                logger.error(msg);
                throw new XmldbException(msg);
            }
        }
        if (!dataDir.canWrite()) {
            String msg = "Database directory not writable: " + dataDir;
            logger.error(msg);
            throw new XmldbException(msg);
        }
    }
    
    
    private void readMetaData() {
        logger.debug(String.format("Read database file '%s'", METADATA_FILE));
        File file = new File(dataDir, METADATA_FILE);
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
        File file = new File(dataDir, METADATA_FILE);
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
        File file = new File(dataDir, COLLECTIONS_FILE);
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
            long timestamp = System.currentTimeMillis();
            rootCollection = new Collection(
                    this, id, "db", timestamp, timestamp, -1);
            // Validation and compession disabled by default.
            rootCollection.setValidationMode(ValidationMode.OFF);
            rootCollection.setCompressionMode(CompressionMode.NONE);
        }
    }
    
    
    private Collection readCollection(DataInputStream dis, int parent)
            throws IOException {
        Collection col;
        
        int colId = dis.readInt();
        String name = dis.readUTF();
        long created = dis.readLong();
        long modified = dis.readLong();
        col = new Collection(this, colId, name, created, modified, parent);

        col.setValidationMode(ValidationMode.values()[dis.readByte()]);
        col.setCompressionMode(CompressionMode.values()[dis.readByte()]);
        
        int noOfIndices = dis.readInt();
        for (int i = 0; i < noOfIndices; i++) {
            col.addIndex(readIndex(dis));
        }

        int noOfDocs = dis.readInt();
        for (int i = 0; i < noOfDocs; i++) {
            Document doc = readDocument(dis, colId);
            int docId = doc.getId();
            documents.put(docId, doc);
            col.addDocument(docId);
        }

        int noOfCols = dis.readInt();
        for (int i = 0; i < noOfCols; i++) {
            Collection childCol = readCollection(dis, colId);
            col.addCollection(childCol.getId());
        }
        
        return col;
    }
    
    
    private Document readDocument(DataInputStream dis, int colId)
            throws IOException {
        int docId = dis.readInt();
        String docName = dis.readUTF();
        MediaType mediaType = MediaType.values()[dis.readByte()];
        long created2 = dis.readLong();
        long modified2 = dis.readLong();
        Document doc = new Document(
                this, docId, docName, mediaType, created2, modified2, colId);
        doc.setCompressionMode(CompressionMode.values()[dis.readByte()]);
        return doc;
    }
    
    
    private Index readIndex(DataInputStream dis) throws IOException {
        int id = dis.readInt();
        String name = dis.readUTF();
        String path = dis.readUTF();
        IndexType type = IndexType.values()[dis.readByte()];
        return new Index(id, name, path, type);
    }
    
    
    private void writeCollections() {
        logger.debug(
                String.format("Write database file '%s'", COLLECTIONS_FILE));
        File file = new File(dataDir, COLLECTIONS_FILE);
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
        dos.writeLong(col.getCreated());
        dos.writeLong(col.getModified());
        dos.writeByte(col.getValidationMode(false).ordinal());
        dos.writeByte(col.getCompressionMode(false).ordinal());
        Set<Index> indices = col.getIndices(false);
        dos.writeInt(indices.size());
        for (Index index : indices) {
            writeIndex(index, dos);
        }
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
    
    
    private void writeIndex(Index index, DataOutputStream dos)
            throws IOException {
        dos.writeInt(index.getId());
        dos.writeUTF(index.getName());
        dos.writeUTF(index.getPath());
        dos.writeByte(index.getType().ordinal());
    }
    
    
    private void writeDocument(Document doc, DataOutputStream dos)
            throws IOException {
        dos.writeInt(doc.getId());
        dos.writeUTF(doc.getName());
        dos.writeByte(doc.getMediaType().ordinal());
        dos.writeLong(doc.getCreated());
        dos.writeLong(doc.getModified());
        dos.writeByte(doc.getCompressionMode().ordinal());
    }
    
    
    private void readIndices() {
        logger.debug(String.format("Read database file '%s'", INDICES_FILE));
        File file = new File(dataDir, INDICES_FILE);
        if (file.exists()) {
            try {
                DataInputStream dis =
                        new DataInputStream(new FileInputStream(file));
                int noOfCols = dis.readInt();
                for (int i = 0; i < noOfCols; i++) {
                	int colId = dis.readInt();
                	Collection col = getCollection(colId);
                	int noOfIndices = dis.readInt();
                	for (int j = 0; j < noOfIndices; j++) {
                		String keyName = dis.readUTF();
                		int noOfValues = dis.readInt();
                		for (int k = 0; k < noOfValues; k++) {
                			Object keyValue = readIndexValue(dis);
                			int noOfDocs = dis.readInt();
                			for (int m = 0; m < noOfDocs; m++) {
                				int docId = dis.readInt();
                				col.addIndexValue(keyName, keyValue, docId);
                			}
                		}
                	}
                }
                dis.close();
            } catch (IOException e) {
                String msg = String.format(
                        "Error reading database file '%s': %s",
                        INDICES_FILE, e.getMessage());
                logger.error(msg);
            }
        }
    }
    
    
    private Object readIndexValue(DataInputStream dis) throws IOException {
        Object value = null;
        IndexType type = IndexType.values()[dis.readByte()];
        if (type == IndexType.STRING) {
            value = dis.readUTF();
        } else if (type == IndexType.INTEGER) { 
            value = dis.readInt();
        } else if (type == IndexType.LONG) { 
            value = dis.readLong();
        } else if (type == IndexType.FLOAT) { 
            value = dis.readFloat();
        } else if (type == IndexType.DOUBLE) { 
            value = dis.readDouble();
        } else if (type == IndexType.DATE) {
            value = new Date(dis.readLong());
        } else {
            throw new IOException(
                    "Error deserializing index value; invalid type: " + type);
        }
        return value;
    }
    
    
    private void writeIndices() {
        logger.debug(
                String.format("Write database file '%s'", INDICES_FILE));
        File file = new File(dataDir, INDICES_FILE);
        try {
            DataOutputStream dos = new DataOutputStream(
                    new FileOutputStream(file));
            dos.writeInt(collections.size());
            for (Collection col : collections.values()) {
            	dos.writeInt(col.getId());
                Map<String, IndexValue> indexValues = col.getIndexValues();
                dos.writeInt(indexValues.size());
                for (String keyName : indexValues.keySet()) {
                    dos.writeUTF(keyName);
                    IndexValue iv = indexValues.get(keyName);
                    Set<Object> values = iv.getValues();
                    dos.writeInt(values.size());
                    for (Object value : values) {
                        writeIndexValue(value, dos);
                        Set<Integer> docs = iv.getDocuments(value);
                        dos.writeInt(docs.size());
                        for (int docId : docs) {
                            dos.writeInt(docId);
                        }
                    }
                }
            }
            dos.close();
        } catch (IOException e) {
            String msg = String.format(
                    "Error writing database file '%s': %s",
                    INDICES_FILE, e.getMessage());
            logger.error(msg);
        }
    }
    
    
    private void writeIndexValue(Object value, DataOutputStream dos)
            throws IOException {
        if (value instanceof String) {
            dos.writeByte(IndexType.STRING.ordinal());
            dos.writeUTF((String) value);
        } else if (value instanceof Integer) {
            dos.writeByte(IndexType.INTEGER.ordinal());
            dos.writeInt((Integer) value);
        } else if (value instanceof Long) {
            dos.writeByte(IndexType.LONG.ordinal());
            dos.writeLong((Long) value);
        } else if (value instanceof Float) {
            dos.writeByte(IndexType.FLOAT.ordinal());
            dos.writeFloat((Float) value);
        } else if (value instanceof Double) {
            dos.writeByte(IndexType.DOUBLE.ordinal());
            dos.writeDouble((Double) value);
        } else if (value instanceof Date) {
            dos.writeByte(IndexType.DATE.ordinal());
            dos.writeLong(((Date) value).getTime());
        } else {
            throw new IOException(
                    "Error serializing index value; invalid type");
        }
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
