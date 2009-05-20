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


import java.io.PrintWriter;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ozsoft.xantippe.Collection;
import org.ozsoft.xantippe.CompressionMode;
import org.ozsoft.xantippe.Database;
import org.ozsoft.xantippe.DatabaseImpl;
import org.ozsoft.xantippe.Document;
import org.ozsoft.xantippe.Util;
import org.ozsoft.xantippe.ValidationMode;


/**
 * Test suite with performance tests.
 * 
 * @author Oscar Stigter
 */
public class PerformanceTest {


    private static final String DATA_DIR = "src/test/resources/data.tmp";
    
    private static final int DOCUMENT_COUNT = 1000;
    
//    private static final String LARGE_DATASET_DIR =
//            "D:/Temp/XML_docs/1000"; 
//            "C:/LocalData/Temp/XML_docs/1000"; 
    
    private static final String XML_HEADER =
		"<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

    private static final Logger logger =
            Logger.getLogger(PerformanceTest.class);

    private static Database database;


    //------------------------------------------------------------------------
    //  Setup & cleanup methods
    //------------------------------------------------------------------------


    @BeforeClass
    public static void beforeClass() {
        Util.initLog4j();
        
        database = new DatabaseImpl();
        database.setDatabaseLocation(DATA_DIR);
    }
    
    
    @Before
    public void before() {
        Util.deleteFile(DATA_DIR);
    }


    @After
    public void after() {
        Util.deleteFile(DATA_DIR);
    }


    //------------------------------------------------------------------------
    //  Tests
    //------------------------------------------------------------------------
    
        @Test
    public void performance() {
        logger.debug("Test suite 'performance' started");
        
        try {
            database.start();
            
            Collection col = database.getRootCollection();
            col.setCompressionMode(CompressionMode.NONE);
            col.setValidationMode(ValidationMode.OFF);
            
            logger.debug("Creating " + DOCUMENT_COUNT + " documents...");
            
            long time = System.currentTimeMillis();
            
            for (int i = 0; i < DOCUMENT_COUNT; i++) {
                String docName = String.format("%08d.xml", i);
                Document doc = col.createDocument(docName);
                PrintWriter writer = new PrintWriter(doc.setContent());
                writer.println(XML_HEADER);
                writer.println("<md:Document xmlns:md=\"http://www.example.com/XMLSchema/MinimalDocument/v1.0\">");
                writer.format("  <Id>%08d</Id>\n", i);
                writer.println("</md:Document>");
                writer.close();
            }
            
            double duration = (System.currentTimeMillis() - time) / 1000.0;
            logger.debug(String.format("Duration: %.3f seconds", duration));
            
            // Restart database to force persist storage
            database.shutdown();
            database.start();
            
            col = database.getRootCollection();
            Assert.assertEquals(DOCUMENT_COUNT, col.getDocuments().size());
            
            database.shutdown();
            
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        
        
        logger.debug("Test suite 'performance' finished");
    }


//      /**
//      * Tests the performance with a large number of documents.
//      * 
//      * This test depends on an external directory with a large dataset.
//      */
//     @Test
//     public void manyDocuments() {
//         logger.debug("Test suite 'manyDocuments' started");
 //
//         try {
//             database.start();
 //
//             Collection rootCol = database.getRootCollection();
//             Collection dataCol = rootCol.createCollection("data");
//             dataCol.setCompressionMode(CompressionMode.DEFLATE);
////             dataCol.addIndex("DocumentId", "//Header/DocumentId", IndexType.STRING);
//             dataCol.addIndex("MachineId", "//Header/MachineID", IndexType.STRING);
 //
//             File dir = new File(LARGE_DATASET_DIR);
//             if (!dir.isDirectory() || !dir.canRead()) {
//                 String msg = "Invalid dataset directory: " + dir;
//                 logger.error(msg);
//                 Assert.fail(msg);
//             }
 //
//             int count = 0;
//             long size = 0L;
 //
//             logger.info("Storing documents...");
//             long startTime = System.currentTimeMillis();
//             for (File file : dir.listFiles()) {
//                 if (file.isFile()) {
//                     Document doc =
//                         dataCol.createDocument(file.getName());
//                     doc.setContent(file);
//                     count++;
//                     size += file.length();
//                 }
//             }
//             double duration = (System.currentTimeMillis() - startTime) / 1000.0;
//             double speed = (size / (1024 * 1024)) / duration;
//             logger.info(String.format(java.util.Locale.US,
//                     "Stored %d documents in %.3f seconds (%.2f MB/s)",
//                     count, duration, speed));
//             
////             logger.info("Finding documents using brute-force search...");
////             String query = "count(collection('/db/data')[element()/Header/MachineID = '1111'])";
////             startTime = System.currentTimeMillis();
////             String result = database.executeQuery(query).toString();
////             duration = (System.currentTimeMillis() - startTime) / 1000.0;
////             Assert.assertEquals(XML_HEADER + "10", result);
////             logger.info(String.format(java.util.Locale.US,
////                     "Searched %d documents in %.3f seconds", count, duration));
//             
//             logger.info("Finding documents using indices...");
//             Key[] keys = new Key[] {
//                     new Key("MachineId", "1111"),
//             };
//             startTime = System.currentTimeMillis();
//             Set<Document> docs = rootCol.findDocuments(keys, true);
//             duration = (System.currentTimeMillis() - startTime) / 1000.0;
//             Assert.assertEquals(10, docs.size());
//             logger.info(String.format(java.util.Locale.US,
//                     "Searched %d documents in %.3f seconds", count, duration));
//             
//             database.shutdown();
 //
//         } catch (XmldbException e) {
//             Assert.fail(e.getMessage());
//         }
 //
//         logger.debug("Test suite 'manyDocuments' finished");
//     }

        
} // DatabaseTest class
