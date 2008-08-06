package xantippe;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test driver for the database.
 * 
 * @author Oscar Stigter
 */
public class DatabaseTest {


	private static final String DATA_DIR = "test/data";

	private static final Logger logger = Logger.getLogger(DatabaseTest.class);

	private static Database database;


	//------------------------------------------------------------------------
	//  JUnit methods
	//------------------------------------------------------------------------


	@BeforeClass
	public static void beforeClass() {
		Util.initLog4j();

		Util.deleteFile(DATA_DIR);

		System.setProperty("xantippe.data.dir", DATA_DIR);

		database  = new DatabaseImpl();
	}


	@AfterClass
	public static void afterClass() {
        Util.deleteFile(DATA_DIR);
	}


	//------------------------------------------------------------------------
	//  Tests
	//------------------------------------------------------------------------


	@Test
	public void test() {
	    File file;
	    
		logger.debug("DatabaseTest started.");

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
			
			doc = fooCol.createDocument("osm_0001.xml");
			doc.setKey("DocumentId", 101);
			doc.setKey("DocumentType", "OnlineStreetMap");
            file = new File("test/docs/osm_0001.xml");
			doc.setContent(file);
            assertEqual(doc.getContent(), file);

            doc = fooCol.createDocument("osm_0002.xml");
            doc.setKey("DocumentId", 102);
            doc.setKey("DocumentType", "OnlineStreetMap");
            file = new File("test/docs/osm_0002.xml");
            doc.setContent(file);
            assertEqual(doc.getContent(), file);
            
//			database.print();
            
			// Find documents by keys.
			Key[] keys = new Key[] {
					new Key("DocumentType", "Foo"),
//					new Key("DocumentId",   2),
			};
			Set<Document> docs = database.findDocuments(keys);
			Assert.assertEquals(2, docs.size());
//			doc = (Document) docs.toArray()[0];
//			Assert.assertEquals("0001.xml", doc.getName());
//			doc = (Document) docs.toArray()[1];
//			Assert.assertEquals("0002.xml", doc.getName());

			database.shutdown();

		} catch (Exception e) {
			logger.error(e);
			Assert.fail(e.getMessage());
		}

		logger.debug("DatabaseTest finished.");
	}


    private static void assertEqual(InputStream is, File file) {
        long startTime = System.currentTimeMillis();
        try {
            InputStream is2 = new FileInputStream(file);
            byte[] buffer1 = new byte[8192];
            byte[] buffer2 = new byte[8192];
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
            logger.debug("File compared in " + duration + " ms");
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }
    
    
}
