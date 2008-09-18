package xantippe;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test suite for the database.
 * 
 * @author Oscar Stigter
 */
public class DatabaseTest {


    private static final String DATA_DIR = "test/dat/data.tmp";
    
//    private static final String LARGE_DATASET_DIR =
//            "D:/Temp/XML_docs/1000"; 
//            "C:/LocalData/Temp/XML_docs/1000"; 
    
    private static final String XML_HEADER =
		"<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

    private static final int BUFFER_SIZE = 8192;  // 8 kB

    private static final Logger logger = Logger.getLogger(DatabaseTest.class);

    private static Database database;


    //------------------------------------------------------------------------
    //  Setup & cleanup methods
    //------------------------------------------------------------------------


    @BeforeClass
    public static void beforeClass() {
        Util.initLog4j();

        System.setProperty("xantippe.data.dir", DATA_DIR);

        database  = new DatabaseImpl();
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
    public void storage() {
        logger.debug("Test suite 'storage' started");

        File file;
        Document doc;
        
        try {
            database.start();
            
            // Create collections.
            Collection rootCol = database.getRootCollection();
            Assert.assertEquals("db", rootCol.getName());
            Assert.assertNull(rootCol.getParent());
            Assert.assertEquals("/db", rootCol.getUri());
            Assert.assertEquals(0, rootCol.getDocuments().size());
            Assert.assertEquals(0, rootCol.getCollections().size());
            Assert.assertEquals(0, rootCol.getIndices(true).size());
            Collection dataCol = rootCol.createCollection("data");
            Assert.assertEquals("data", dataCol.getName());
            Assert.assertEquals("/db/data", dataCol.getUri());
            Assert.assertEquals("/db", dataCol.getParent().getUri());
            Assert.assertEquals(0, dataCol.getDocuments().size());
            Assert.assertEquals(0, dataCol.getCollections().size());
            Assert.assertEquals(0, dataCol.getIndices(true).size());
            Collection fooCol = dataCol.createCollection("foo");
            Assert.assertEquals("foo", fooCol.getName());
            Assert.assertEquals("/db/data/foo", fooCol.getUri());
            Assert.assertEquals("/db/data", fooCol.getParent().getUri());
            Assert.assertEquals(0, fooCol.getDocuments().size());
            Assert.assertEquals(0, fooCol.getCollections().size());
            Assert.assertEquals(0, fooCol.getIndices(true).size());
            Collection barCol = dataCol.createCollection("bar");
            Assert.assertEquals("bar", barCol.getName());
            Assert.assertEquals("/db/data/bar", barCol.getUri());
            Assert.assertEquals("/db/data", barCol.getParent().getUri());
            Assert.assertEquals(0, barCol.getDocuments().size());
            Assert.assertEquals(0, barCol.getCollections().size());
            Assert.assertEquals(0, barCol.getIndices(true).size());
            Collection emptyCol = dataCol.createCollection("empty");
            Assert.assertEquals("/db/data/empty", emptyCol.getUri());

            // Create indices.
            Assert.assertEquals(0, rootCol.getIndices(true).size());
            Assert.assertEquals(0, dataCol.getIndices(true).size());
            Assert.assertEquals(0, fooCol.getIndices(true).size());
            dataCol.addIndex(
            		"DocumentId", "//Header/Id", IndexType.INTEGER);
            Assert.assertEquals(1, dataCol.getIndices(true).size());
            Index index = dataCol.getIndex("DocumentId");
            Assert.assertNotNull(index);
            Assert.assertEquals("DocumentId", index.getName());
            Assert.assertEquals("//Header/Id", index.getPath());
            Assert.assertEquals(IndexType.INTEGER, index.getType());
            dataCol.addIndex(
            		"DocumentType", "//Header/Type", IndexType.STRING);
            dataCol.addIndex(
            		"DocumentVersion", "//Header/Version", IndexType.STRING);
            Assert.assertEquals(3, dataCol.getIndices(false).size());
            Assert.assertEquals(3, dataCol.getIndices(true).size());
            Assert.assertEquals(0, fooCol.getIndices(false).size());
            Assert.assertEquals(3, fooCol.getIndices(true).size());
            index = fooCol.getIndex("DocumentId");
            Assert.assertNotNull(index);
            Assert.assertEquals("DocumentId", index.getName());
            Assert.assertEquals("//Header/Id", index.getPath());
            index = fooCol.getIndex("DocumentType");
            Assert.assertNotNull(index);
            Assert.assertEquals("DocumentType", index.getName());
            Assert.assertEquals("//Header/Type", index.getPath());
            index = fooCol.getIndex("DocumentVersion");
            Assert.assertNotNull(index);
            Assert.assertEquals("DocumentVersion", index.getName());
            Assert.assertEquals("//Header/Version", index.getPath());
            index = fooCol.getIndex("NonExisting");
            Assert.assertNull(index);

            // Add documents.
            
            file = new File("test/dat/db/data/foo/Foo-0001.xml");
            doc = fooCol.createDocument(file.getName());
            Assert.assertEquals("Foo-0001.xml", doc.getName());
            doc.setContent(file);
            assertEqual(doc.getContent(), file);
            
            file = new File("test/dat/db/data/foo/Foo-0002.xml");
            doc = fooCol.createDocument(file.getName());
            doc.setContent(file);
            assertEqual(doc.getContent(), file);
            
            doc = barCol.createDocument("Bar-0001.xml");
            OutputStream os = doc.setContent();
            os.write(("<BarDocument><Header><Id>3</Id><Type>Bar</Type>"
            		+ "<Version>v1.0</Version></Header></BarDocument>").getBytes());
            os.close();
            
            // Shutdown and restart database to test persistency.
            database.shutdown();
            database.start();
            
            // Refresh collection references.
            rootCol = database.getRootCollection();
            dataCol = database.getCollection("/db/data");
            fooCol = dataCol.getCollection("foo");
            
            // Find documents by keys (using indices).
            
            Key[] keys = new Key[] {
            		new Key("DocumentId", 2)
            };
            Set<Document> docs = rootCol.findDocuments(keys, true);
            Assert.assertEquals(1, docs.size());
            doc = (Document) docs.toArray()[0];
            Assert.assertEquals("/db/data/foo/Foo-0002.xml", doc.getUri());

            keys = new Key[] {
                    new Key("DocumentType", "Foo")
            };
            docs = rootCol.findDocuments(keys, true);
            Assert.assertEquals(2, docs.size());
            doc = (Document) docs.toArray()[0];
            Assert.assertEquals("/db/data/foo/Foo-0001.xml", doc.getUri());
            doc = (Document) docs.toArray()[1];
            Assert.assertEquals("/db/data/foo/Foo-0002.xml", doc.getUri());
            docs = fooCol.findDocuments(keys, false);
            Assert.assertEquals(2, docs.size());

            keys = new Key[] {
                    new Key("DocumentVersion", "v1.0")
            };
            docs = rootCol.findDocuments(keys, true);
            Assert.assertEquals(2, docs.size());
            doc = (Document) docs.toArray()[0];
            Assert.assertEquals("/db/data/bar/Bar-0001.xml", doc.getUri());
            doc = (Document) docs.toArray()[1];
            Assert.assertEquals("/db/data/foo/Foo-0001.xml", doc.getUri());
            
            docs = fooCol.findDocuments(keys, true);
            Assert.assertEquals(1, docs.size());
            doc = (Document) docs.toArray()[0];
            Assert.assertEquals("/db/data/foo/Foo-0001.xml", doc.getUri());
            
            keys = new Key[] {
                    new Key("DocumentType", "Foo")
            };
            docs = dataCol.findDocuments(keys, false);
            Assert.assertEquals(0, docs.size());

            keys = new Key[] {
                    new Key("DocumentType", "NonExisting"),
            };
            docs = rootCol.findDocuments(keys, true);
            Assert.assertEquals(0, docs.size());

            keys = new Key[] {
                    new Key("DocumentId", 2),
                    new Key("DocumentType", "NonExisting"),
            };
            docs = rootCol.findDocuments(keys, true);
            Assert.assertEquals(0, docs.size());
            
            // Delete documents and collections.
            
            Assert.assertFalse(fooCol.deleteDocument("NonExisting"));
            Assert.assertFalse(fooCol.deleteCollection("NonExisting"));
            
            Assert.assertTrue(fooCol.deleteDocument("Foo-0001.xml"));
            try {
                database.getDocument("/db/data/foo/Foo-0001.xml");
                Assert.fail("No exception thrown!");
            } catch (XmldbException e) {
                // OK
            }

            // Delete an empty collection.
            Assert.assertTrue(dataCol.deleteCollection("empty"));
            try {
                database.getCollection("/db/data/empty");
                Assert.fail("No exception thrown!");
            } catch (XmldbException e) {
                // OK
            }

            // Delete a single, non-empty collection.
            Assert.assertTrue(dataCol.deleteCollection("foo"));
            try {
                database.getCollection("/db/data/foo");
                Assert.fail("No exception thrown!");
            } catch (XmldbException e) {
                // OK
            }

            // Delete a non-empty collection tree (recursive).
            Assert.assertTrue(rootCol.deleteCollection("data"));
            try {
                database.getCollection("/db/data");
                Assert.fail("No exception thrown!");
            } catch (XmldbException e) {
                // OK
            }
            
            database.shutdown();
            
        } catch (XmldbException e) {
            Assert.fail(e.getMessage());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        logger.debug("Test suite 'storage' finished");
    }

    
//    @Test
//    public void manyDocuments() {
//        logger.debug("Test suite 'manyDocuments' started");
//
//        try {
//            database.start();
//
//            Collection rootCol = database.getRootCollection();
//            Collection dataCol = rootCol.createCollection("data");
////            dataCol.addIndex("DocumentId", "//Header/DocumentId", IndexType.STRING);
//            dataCol.addIndex("MachineId", "//Header/MachineID", IndexType.STRING);
//
//            File dir = new File(LARGE_DATASET_DIR);
//            if (!dir.isDirectory() || !dir.canRead()) {
//                String msg = "Invalid dataset directory: " + dir;
//                logger.error(msg);
//                Assert.fail(msg);
//            }
//
//            int count = 0;
//            long size = 0L;
//
//            logger.info("Storing documents...");
//            long startTime = System.currentTimeMillis();
//            for (File file : dir.listFiles()) {
//                if (file.isFile()) {
//                    Document doc =
//                        dataCol.createDocument(file.getName());
//                    doc.setContent(file);
//                    count++;
//                    size += file.length();
//                }
//            }
//            double duration = (System.currentTimeMillis() - startTime) / 1000.0;
//            double speed = (size / (1024 * 1024)) / duration;
//            logger.info(String.format(java.util.Locale.US,
//                    "Stored %d documents in %.3f seconds (%.2f MB/s)",
//                    count, duration, speed));
//            
////            logger.info("Finding documents using brute-force search...");
////            String query = "count(collection('/db/data')[element()/Header/MachineID = '1111'])";
////            startTime = System.currentTimeMillis();
////            String result = database.executeQuery(query).toString();
////            duration = (System.currentTimeMillis() - startTime) / 1000.0;
////            Assert.assertEquals(XML_HEADER + "10", result);
////            logger.info(String.format(java.util.Locale.US,
////                    "Searched %d documents in %.3f seconds", count, duration));
//            
//            logger.info("Finding documents using indices...");
//            Key[] keys = new Key[] {
//                    new Key("MachineId", "1111"),
//            };
//            startTime = System.currentTimeMillis();
//            Set<Document> docs = rootCol.findDocuments(keys, true);
//            duration = (System.currentTimeMillis() - startTime) / 1000.0;
//            Assert.assertEquals(10, docs.size());
//            logger.info(String.format(java.util.Locale.US,
//                    "Searched %d documents in %.3f seconds", count, duration));
//            
//            database.shutdown();
//
//        } catch (XmldbException e) {
//            Assert.fail(e.getMessage());
//        }
//
//        logger.debug("Test suite 'manyDocuments' finished");
//    }


    @Test
    public void validation() {
        logger.debug("Test suite 'validation' started");
        
        File file;
        Document doc;

        try {
            database.start();

            Collection col = database.getRootCollection();
            col.setValidationMode(ValidationMode.ON);
            
            // Self-contained schema.
            file = new File("test/dat/db/schemas/Address_v1.0.xsd");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            file = new File("test/dat/db/data/addressbook/Address_0001.xml");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            file = new File("test/dat/db/data/addressbook/Address_0002.xml");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            
            // Multi-file schema (with 'include' statement).
            file = new File("test/dat/db/schemas/Generic_v1.0.xsd");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            file = new File("test/dat/db/schemas/TestResult_v1.0.xsd");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            file = new File("test/dat/db/data/testresults/TestResult_0001.xml");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            file = new File("test/dat/db/data/testresults/TestResult_0002.xml");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            file = new File("test/dat/db/data/testresults/TestResult_0003.xml");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            
            database.shutdown();
            
        } catch (XmldbException e) {
            Assert.fail(e.getMessage());
        }

        logger.debug("Test suite 'validation' finished");
    }


    @Test
    public void xquery() {
        logger.debug("Test suite 'xquery' started");
        
        File file;
        Document doc;

        try {
            database.start();

            // Fill database.
            Collection rootCol = database.getRootCollection();
            Collection dataCol = rootCol.createCollection("data");
            Collection fooCol = dataCol.createCollection("foo");
            file = new File("test/dat/db/data/foo/Foo-0001.xml");
            doc = fooCol.createDocument(file.getName());
            doc.setContent(file);
            file = new File("test/dat/db/data/foo/Foo-0002.xml");
            doc = fooCol.createDocument(file.getName());
            doc.setContent(file);
            Collection barCol = dataCol.createCollection("bar");
            file = new File("test/dat/db/data/bar/Bar-0001.xml");
            doc = barCol.createDocument(file.getName());
            doc.setContent(file);
            Collection modulesCol = rootCol.createCollection("modules");
            file = new File("test/dat/db/modules/greeting.xqy");
            doc = modulesCol.createDocument(file.getName());
            doc.setContent(file);
        	
        	// Simple query.
            String query = "<Result>{2 + 3}</Result>";
	        String result = database.executeQuery(query).toString();
	        Assert.assertEquals(XML_HEADER + "\n<Result>5</Result>", result);
	        
	        // Local function call.
	        query = "declare namespace m = 'urn:math';\n\n" +
			        "declare function m:sum($x as xs:integer, $y as xs:integer) as xs:integer { \n" +
			        "  let $result := $x + $y \n" +
			        "  return $result \n" +
			        "}; \n\n" +
			        "<Result>{m:sum(2, 3)}</Result>";
            result = database.executeQuery(query).toString();
            Assert.assertEquals(XML_HEADER + "\n<Result>5</Result>", result);
            
	        // doc() function.
	        query = "doc('/db/data/foo/Foo-0001.xml')/element()/Header/Id/text()";
            result = database.executeQuery(query).toString();
            Assert.assertEquals(XML_HEADER + "1", result);
            
            // collection() function.
            query = "count(collection('/db/data?recurse=yes'))";
            result = database.executeQuery(query).toString();
            Assert.assertEquals(XML_HEADER + "3", result);
            query = "collection('/db/data/foo')/element()/Header/Id/text()";
            result = database.executeQuery(query).toString();
            Assert.assertEquals(XML_HEADER + "12", result);
            
            // Stored XQuery module
            query = "import module namespace gr = 'http://www.example.com/greeting' \n" +
                    "  at '/db/modules/greeting.xqy'; \n\n" +
                    "gr:greeting('Mr Smith')";
            result = database.executeQuery(query).toString();
            Assert.assertEquals(XML_HEADER + "Hello, Mr Smith!", result);
            
            database.shutdown();
            
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        
        logger.debug("Test suite 'xquery' finished");
    }
    
    
    //------------------------------------------------------------------------
    //  Private methods.
    //------------------------------------------------------------------------


    private static void assertEqual(InputStream is, File file) {
        try {
            InputStream is2 = new FileInputStream(file);
            byte[] buffer1 = new byte[BUFFER_SIZE];
            byte[] buffer2 = new byte[BUFFER_SIZE];
            int bytesRead1;
            int bytesRead2;
            while ((bytesRead1 = is.read(buffer1)) > 0) {
                bytesRead2 = is2.read(buffer2);
                Assert.assertEquals(bytesRead1, bytesRead2);
                Assert.assertArrayEquals(buffer1, buffer2);
            }
            is2.close();
            is.close();
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }
    
    
}
