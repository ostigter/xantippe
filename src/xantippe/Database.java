package xantippe;


import java.io.OutputStream;


/**
 * Lightweight XML database with validation, XQuery and indexing, using a plain
 * file system store.
 * 
 * @author Oscar Stigter
 */
public interface Database {
    
    
    /**
     * Sets the database location.
     * 
     * @param path  the database location
     */
    void setDatabaseLocation(String path);


    /**
     * Starts the database.
     * 
     * @throws XmldbException
     *             if the database was already running
     */
    void start() throws XmldbException;

    
    /**
     * Shuts down the database.
     * 
     * @throws XmldbException
     *             if the database is not running
     */
    void shutdown() throws XmldbException;

    
    /**
     * Returns whether the database is running.
     * 
     * @return whether the database is running
     */
    boolean isRunning();

    
    /**
     * Returns the root collection.
     * 
     * @return  the root collection
     * 
     * @throws XmldbException
     *             if the database is not running
     */
    Collection getRootCollection() throws XmldbException;
    
    
    /**
     * Returns the collection with the specified URI.
     * 
     * @param uri  the URI
     * 
     * @return  the collection, of null it not found
     * 
     * @throws XmldbException  if any error occurred
     */
    Collection getCollection(String uri) throws XmldbException;

    
    /**
     * Returns the document with the specified URI.
     * 
     * @param uri  the URI
     * 
     * @return  the document, of null it not found
     * 
     * @throws XmldbException  if any error occurred
     */
    Document getDocument(String uri) throws XmldbException;

    
    /**
     * Execute the specified query.
     * 
     * @param  query  the query
     * 
     * @return  OutputStream with the query results
     * 
     * @throws XmldbException  if the query could not be executed
     */
    OutputStream executeQuery(String query) throws XmldbException;
    
    
    // Debug only
    public void print();

    
}
