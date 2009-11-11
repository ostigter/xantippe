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
     * @return The database location.
     */
    String getDatabaseLocation();
    
    /**
     * Sets the database location.
     * 
     * This method may only be called when the database is not running.
     * 
     * @param path
     *            The database location.
     * 
     * @throws IllegalArgumentException
     *             If the path is null or empty.
     * @throws IllegalStateException
     *             If the database is running.
     */
    void setDatabaseLocation(String path);

    /**
     * Starts the database.
     * 
     * @throws IllegalStateException
     *             If the database is already running.
     */
    void start() throws XmldbException;

    /**
     * Shuts down the database.
     * 
     * @throws IllegalStateException
     *             If the database is not running.
     */
    void shutdown() throws XmldbException;
    
    /**
     * Indicates whether the database is running.
     * 
     * @return True if running, otherwise false.
     */
    boolean isRunning();
    
    /**
     * Returns the root collection.
     * 
     * @return The root collection.
     * 
     * @throws IllegalStateException
     *             If the database is not running.
     */
    Collection getRootCollection() throws XmldbException;
    
    /**
     * Returns whether a specific resource exists.
     * 
     * @param uri
     *            The resource URI.
     * 
     * @return True if the resource exists, otherwise false.
     * 
     * @throws IllegalStateException
     *             If the database is not running.
     * @throws IllegalArgumentException
     *             If the URI is null or empty.
     */
    boolean exists(String uri);
    
    /**
     * Returns whether a specific resource is a collection.
     * 
     * If the resource does not exist it returns false.
     * 
     * @param uri The resource URI.
     * 
     * @return True if a collection, otherwise false.
     * 
     * @throws IllegalStateException
     *             If the database is not running.
     * @throws IllegalArgumentException
     *             If the URI is null or empty.
     */
    boolean isCollection(String uri);
    
    /**
     * Returns whether a specific resource is a document.
     * 
     * If the resource does not exist it returns false.
     * 
     * @param uri
     *            The resource URI.
     * 
     * @return True if a document, otherwise false.
     * 
     * @throws IllegalStateException
     *             If the database is not running.
     * @throws IllegalArgumentException
     *             If the URI is null or empty.
     */
    boolean isDocument(String uri);
    
    /**
     * Returns the collection with the specified URI.
     * 
     * @param uri
     *            The collection URI.
     * 
     * @return The collection.
     * 
     * @throws IllegalStateException
     *             If the database is not running.
     * @throws IllegalArgumentException
     *             If the URI is null or empty.
     * @throws XmldbException
     *             If the collection does not exist, or the URI is invalid.
     */
    Collection getCollection(String uri) throws XmldbException;
    
    /**
     * Returns the document with the specified URI.
     * 
     * @param uri
     *            The document URI.
     * 
     * @return The document.
     * 
     * @throws IllegalStateException
     *             If the database is not running.
     * @throws IllegalArgumentException
     *             If the URI is null or empty.
     * @throws XmldbException
     *             If the document does not exist, or the URI is invalid.
     */
    Document getDocument(String uri) throws XmldbException;
    
    /**
     * Executes a query.
     * 
     * The query must be in the form of an XQuery expression.
     * 
     * The query result (plain text) is written to the returned OutputStream.
     * 
     * @param query
     *            The query.
     * 
     * @return The OutputStream with the query results.
     * 
     * @throws IllegalStateException
     *             If the database is not running.
     * @throws IllegalArgumentException
     *             If the query text is null or empty.
     * @throws XmldbException
     *             If the query could not be executed due to errors.
     */
    OutputStream executeQuery(String query) throws XmldbException;
    
}
