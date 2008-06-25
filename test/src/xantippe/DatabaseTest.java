package xantippe;


import org.apache.log4j.Logger;
import org.junit.AfterClass;
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
		logger.debug("DatabaseTest started.");
		
		try {
		    database.start();
		    
    		// Create collections.
		    Collection rootCol = database.getRootCollection();
		    Collection dataCol = rootCol.createCollection("data");
		    Collection fooCol = dataCol.createCollection("Foo");
		    Collection modulesCol = rootCol.createCollection("modules");
    		
//		    // Create indices.
//		    dataCol.addIndex("DocumentId", "/Document/Id");
//		    fooCol.addIndex("DocumentType", "/Document/Type");
//		    logger.info(dataCol.getIndices());
//		    logger.info(fooCol.getIndices());
		    
//    		// Add documents.
//    		Document doc = fooCol.createDocument("0001.xml");
//    		doc.setKey("DocumentId", 1);
//    		doc.setKey("DocumentType", "Foo");
//    		doc.setContent("<Document>\n  <Id>1</Id>\n  <Type>Foo</Type>\n</Document>");
//    		doc = fooCol.createDocument("0002.xml");
//    		doc.setKey("DocumentId", 2);
//    		doc.setKey("DocumentType", "Foo");
//    		doc.setContent("<Document>\n  <Id>2</Id>\n  <Type>Foo</Type>\n</Document>");
//    		doc = fooCol.createDocument("0003.xml");
//    		doc.setKey("DocumentId", 3);
//    		doc.setKey("DocumentType", "Bar");
//    		doc.setContent("<Document>\n  <Id>3</Id>\n  <Type>Bar</Type>\n</Document>");
//            doc = modulesCol.createDocument("main.xqy");
//            doc.setContent("");
    
//            // Find documents by keys.
//            Key[] keys = new Key[] {
//            		new Key("DocumentType", "Foo"),
////                    new Key("DocumentId",   2),
//    		};
//    		Set<Document> docs = database.findDocuments(keys);
//            if (docs.size() == 0) {
//                System.out.println("No document found.");
//            } else {
//                System.out.println("Documents found:");
//                for (Document d : docs) {
//                    System.out.println("  " + d);
//                }
//            }
            
            database.print();
            
            database.shutdown();
            
		} catch (XmldbException e) {
			logger.error(e);
		}
		
		logger.debug("DatabaseTest finished.");
	}


}
