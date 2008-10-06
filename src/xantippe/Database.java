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
     * Returns the database location.
     * 
     * @return  the database location
     */
    String getDatabaseLocation();
    
    
    /**
     * Sets the database location.
     * 
     * This method may only be called when the database is not running. 
     * 
     * @param  path  the database location
     * 
     * @throws  IllegalArgumentException  if the path is null or empty
     * @throws  IllegalStateException     if the database is running
     */
    void setDatabaseLocation(String path);


    /**
     * Starts the database.
     * 
     * @throws  IllegalStateException
     *              if the database is already running
     */
    void start() throws XmldbException;

    
    /**
     * Shuts down the database.
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     */
    void shutdown() throws XmldbException;

    
    /**
     * Indicates whether the database is running.
     * 
     * @return  true if the database is running, otherwise false
     */
    boolean isRunning();

    
    /**
     * Returns the root collection.
     * 
     * @return  the root collection
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     */
    Collection getRootCollection() throws XmldbException;
    
    
    /**
     * Returns the collection with the specified URI.
     * 
     * @param   uri  the collection URI
     * 
     * @return  the collection
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     * @throws  IllegalArgumentException
     *              if the URI is null or empty
     * @throws  XmldbException
     *              if the collection does not exist, or the URI is invalid 
     */
    Collection getCollection(String uri) throws XmldbException;

    
    /**
     * Returns the document with the specified URI.
     * 
     * @param   uri  the document URI
     * 
     * @return  the document
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     * @throws  IllegalArgumentException
     *              if the URI is null or empty
     * @throws  XmldbException
     *              if the document does not exist, or the URI is invalid 
     */
    Document getDocument(String uri) throws XmldbException;

    
    /**
     * Executes the specified query.
     * 
     * The query must be in the form of an XQuery expression.
     * 
     * The query result (plain text) is written to the returned OutputStream.
     * 
     * @param   query  the query body
     * 
     * @return  OutputStream with the query results
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     * @throws  IllegalArgumentException
     *              if the query text is null or empty
     * @throws  XmldbException
     *              if the query could not be executed
     */
    // TODO: Add query parameters
    // TODO: Offer other result formats than plain text OutputStream
    OutputStream executeQuery(String query) throws XmldbException;
    
    
    /**
     * Prints a listing of the complete database on the console.
     * 
     * This method is meant for debugging purposes only.
     */
    public void print();

    
}
