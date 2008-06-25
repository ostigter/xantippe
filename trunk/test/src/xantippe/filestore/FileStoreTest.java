package xantippe.filestore;


import java.io.File;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import xantippe.Util;


public class FileStoreTest {
	

    private static final String DATA_DIR = "test/data";

    private static final String DOCS_DIR = "test/docs";
    
	
    //------------------------------------------------------------------------
    //  JUnit methods
    //------------------------------------------------------------------------
    
    
	@BeforeClass
	public static void beforeClass() {
		Util.initLog4j();
		Util.deleteFile(DATA_DIR);
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
		FileStore store = new FileStore(DATA_DIR);

		try {
			
			store.start();
			
			Assert.assertEquals(0, store.size());
			
			// Add file.
			store.store(1, new File(DOCS_DIR + "/0001.xml"));
			Assert.assertEquals(1, store.size());
			
			// Add file.
			store.store(2, new File(DOCS_DIR + "/0002.xml"));
			Assert.assertEquals(2, store.size());
			
			// Update file.
			store.store(2, new File(DOCS_DIR + "/0002.xml"));
			Assert.assertEquals(2, store.size());
			
			// Add file.
			store.store(3, new File(DOCS_DIR + "/0003.xml"));
			Assert.assertEquals(3, store.size());
			
			// Delete file.
			store.delete(2);
			Assert.assertEquals(2, store.size());
			
			// Delete all files.
			store.deleteAll();
			Assert.assertEquals(0, store.size());
			
			store.shutdown();
			
		} catch (FileStoreException e) {
			Assert.fail(e.getMessage());
		}
	}
	
	
}
