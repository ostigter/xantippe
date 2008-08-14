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


    private static final String DATA_DIR = "test/data";
    
//  private static final String LARGE_DATASET_DIR =
//          "D:/Temp/XML_docs/1000"; 
//          "C:/LocalData/Temp/XML_docs/1000"; 
    
    private static final int BUFFER_SIZE = 8192;  // 8 kB

    private static final Logger logger = Logger.getLogger(DatabaseTest.class);

    private static Database database;


    //------------------------------------------------------------------------
    //  JUnit methods
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
    public void basics() {
        logger.debug("Test suite 'basics' started.");

        File file;
        
        try {
            database.start();

            // Create collections.
            Collection rootCol = database.getRootCollection();
            Assert.assertEquals(1, rootCol.getId());
            Assert.assertEquals("db", rootCol.getName());
            Assert.assertNull(rootCol.getParent());
            Assert.assertEquals("/db", rootCol.getUri());
            Assert.assertEquals(0, rootCol.getDocuments().size());
            Assert.assertEquals(0, rootCol.getCollections().size());
            Assert.assertEquals(0, rootCol.getIndices().size());
            Collection dataCol = rootCol.createCollection("data");
            Assert.assertEquals(2, dataCol.getId());
            Assert.assertEquals("data", dataCol.getName());
            Assert.assertEquals(1, dataCol.getParent().getId());
            Assert.assertEquals("/db/data", dataCol.getUri());
            Assert.assertEquals(0, dataCol.getDocuments().size());
            Assert.assertEquals(0, dataCol.getCollections().size());
            Assert.assertEquals(0, dataCol.getIndices().size());
            Collection fooCol = dataCol.createCollection("Foo");
            Assert.assertEquals(3, fooCol.getId());
            Assert.assertEquals("Foo", fooCol.getName());
            Assert.assertEquals(2, fooCol.getParent().getId());
            Assert.assertEquals("/db/data/Foo", fooCol.getUri());
            Assert.assertEquals(0, fooCol.getDocuments().size());
            Assert.assertEquals(0, fooCol.getCollections().size());
            Assert.assertEquals(0, fooCol.getIndices().size());
            Collection modulesCol = rootCol.createCollection("modules");
            Assert.assertEquals(4, modulesCol.getId());
            Assert.assertEquals("modules", modulesCol.getName());
            Assert.assertEquals(1, modulesCol.getParent().getId());
            Assert.assertEquals("/db/modules", modulesCol.getUri());
            Assert.assertEquals(0, modulesCol.getDocuments().size());
            Assert.assertEquals(0, modulesCol.getCollections().size());
            Assert.assertEquals(0, modulesCol.getIndices().size());

            // Create indices.
            dataCol.addIndex("DocumentId", "/Document/Id");
            Assert.assertEquals(1, dataCol.getIndices().size());
            Index index = dataCol.getIndex("DocumentId");
            Assert.assertNotNull(index);
            Assert.assertEquals(5, index.getId());
            Assert.assertEquals("DocumentId", index.getName());
            Assert.assertEquals("/Document/Id", index.getPath());
            fooCol.addIndex("DocumentType", "/Document/Type");
            Assert.assertEquals(2, fooCol.getIndices().size());
            index = fooCol.getIndex("DocumentId");
            Assert.assertNotNull(index);
            index = fooCol.getIndex("DocumentType");
            Assert.assertNotNull(index);
            Assert.assertEquals(6, index.getId());
            Assert.assertEquals("DocumentType", index.getName());
            Assert.assertEquals("/Document/Type", index.getPath());

            // Add documents.
            Document doc = fooCol.createDocument("0001.xml");
            Assert.assertEquals(7, doc.getId());
            Assert.assertEquals("0001.xml", doc.getName());
            Assert.assertEquals(3, doc.getParent().getId());
            Assert.assertEquals("/db/modules", modulesCol.getUri());
            Assert.assertEquals(0, doc.getKeys().length);
            doc.setKey("DocumentId", 1);
            doc.setKey("DocumentType", "Foo");
            file = new File("test/docs/0001.xml");
            doc.setContent(file);
            assertEqual(doc.getContent(), file);
            
            doc = fooCol.createDocument("0002.xml");
            doc.setKey("DocumentId", 2);
            doc.setKey("DocumentType", "Foo");
            file = new File("test/docs/0002.xml");
            doc.setContent(file);
            assertEqual(doc.getContent(), file);

            doc = fooCol.createDocument("0003.xml");
            doc.setKey("DocumentId", 3);
            doc.setKey("DocumentType", "Bar");
            OutputStream os = doc.setContent();
            os.write("<Document>\n  <Id>3</Id>\n  <Type>Bar</Type>\n</Document>".getBytes());
            os.close();
            
            // Find documents by keys.
            Key[] keys = new Key[] {
                    new Key("DocumentType", "Foo"),
//                  new Key("DocumentId",   2),
            };
            //TODO: Use collection recursion.
            Set<Document> docs = fooCol.findDocuments(keys);
            Assert.assertEquals(2, docs.size());
//          doc = (Document) docs.toArray()[0];
//          Assert.assertEquals("0001.xml", doc.getName());
//          doc = (Document) docs.toArray()[1];
//          Assert.assertEquals("0002.xml", doc.getName());

            database.shutdown();
            
        } catch (XmldbException e) {
            Assert.fail(e.getMessage());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        logger.debug("Test suite 'basics' finished.");
    }
    
    
//  @Test
//  public void manyDocuments() {
//      logger.debug("Test suite 'manyDocuments' started.");
//
//      try {
//          database.start();
//
//          Collection col = database.getRootCollection();
//
//          File dir = new File(LARGE_DATASET_DIR);
//          if (!dir.isDirectory() || !dir.canRead()) {
//              String msg = "Invalid dataset directory: " + dir;
//              logger.error(msg);
//              Assert.fail(msg);
//          }
//
//          int count = 0;
//          long size = 0L;
//
//          logger.info("Storing documents...");
//          long startTime = System.currentTimeMillis();
//          for (File file : dir.listFiles()) {
//              if (file.isFile()) {
//                  Document doc =
//                      col.createDocument(file.getName());
//                  doc.setContent(file);
//                  count++;
//                  size += file.length();
//              }
//          }
//          double duration = (System.currentTimeMillis() - startTime) / 1000.0;
//          double speed = (size / (1024 * 1024)) / duration;
//          logger.info(String.format(Locale.US,
//                  "Stored %d documents in %.3f seconds (%.2f MB/s)",
//                  count, duration, speed));
//
//          database.shutdown();
//
//      } catch (XmldbException e) {
//          Assert.fail(e.getMessage());
//      }
//
//      logger.debug("Test suite 'manyDocuments' finished.");
//  }


    @Test
    public void validation() {
        logger.debug("Test suite 'validation' started.");
        
        File file;
        Document doc;

        try {
            database.start();

            Collection col = database.getRootCollection();
            col.setValidationMode(ValidationMode.ON);
            
            // Self-contained schema.
            file = new File("test/schemas/Address_v1.0.xsd");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            file = new File("test/docs/Address_0001.xml");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            file = new File("test/docs/Address_0002.xml");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            
            // Multi-file schema (with 'include' statement).
            file = new File("test/schemas/Generic_v1.0.xsd");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            file = new File("test/schemas/TestResult_v1.0.xsd");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            file = new File("test/docs/TestResult_0001.xml");
            doc = col.createDocument(file.getName());
            doc.setContent(file);
            
            database.shutdown();
            
        } catch (XmldbException e) {
            Assert.fail(e.getMessage());
        }

        logger.debug("Test suite 'validation' finished.");
    }


    //------------------------------------------------------------------------
    //  Private methods.
    //------------------------------------------------------------------------


    private static void assertEqual(InputStream is, File file) {
        logger.debug("Verifying document content...");
        long startTime = System.currentTimeMillis();
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
            long duration = System.currentTimeMillis() - startTime;
            logger.debug("Document verified in " + duration + " ms");
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }
    
    
}
