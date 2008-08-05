package xantippe.filestore;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
	    File file;
	    
		FileStore store = new FileStore(DATA_DIR);

		try {
			
			store.start();
			
			Assert.assertEquals(0, store.size());
			
			// Add file.
			store.store(1, new File(DOCS_DIR + "/0001.xml"));
			Assert.assertEquals(1, store.size());
			
            // Retrieve file.
            InputStream is = store.retrieve(1);
            Assert.assertNotNull(is);
            Assert.assertEquals(83, is.available());
            Assert.assertEquals('<', (char) is.read());
            Assert.assertEquals(82, is.available());
            Assert.assertEquals('D', (char) is.read());
            Assert.assertEquals(81, is.available());
            Assert.assertEquals('o', (char) is.read());
            Assert.assertEquals(80, is.available());
            Assert.assertEquals('c', (char) is.read());
            Assert.assertEquals(79, is.available());
            byte[] buffer = new byte[79];
            int read = is.read(buffer, 0, 6);
            Assert.assertEquals(6, read);
            Assert.assertEquals("ument>", new String(buffer, 0, read));
            read = is.read(buffer);
            Assert.assertEquals(73, read);
            is.close();
            
			// Add file.
            file = new File(DOCS_DIR + "/0002.xml");
			store.store(2, file);
			Assert.assertEquals(2, store.size());
			assertEqual(store.retrieve(2), file);
			
			// Update file.
			store.store(2, file);
			Assert.assertEquals(2, store.size());
            assertEqual(store.retrieve(2), file);
			
			// Add file.
            file = new File(DOCS_DIR + "/0003.xml");
            store.store(3, file);
			Assert.assertEquals(3, store.size());
            assertEqual(store.retrieve(3), file);
			
            // Add file.
            file = new File(DOCS_DIR + "/osm_0002.xml");
            store.store(4, file);
            Assert.assertEquals(4, store.size());
            assertEqual(store.retrieve(4), file);
            
//            // Add file.
//            inFile = new File(DOCS_DIR + "/osm_0001.xml");
//            store.store(4, inFile);
//            Assert.assertEquals(4, store.size());
//            outFile = new File("test/docs/osm_0001-out.xml"); 
//            writeInputStreamToFile(store.retrieve(1), outFile);
//            assertEqual(inFile, outFile);
            
//			// Delete file.
//			store.delete(2);
//			Assert.assertEquals(2, store.size());
//			
//			// Delete all files.
//			store.deleteAll();
//			Assert.assertEquals(0, store.size());
			
			store.shutdown();
			
		} catch (FileStoreException e) {
		    System.err.println(e);
			Assert.fail(e.getMessage());
        } catch (IOException e) {
            System.err.println(e);
            Assert.fail(e.getMessage());
        }
	}
	
	
//	private static void writeInputStreamToFile(InputStream is, File file)
//	        throws IOException {
//	    OutputStream os = new FileOutputStream(file);
//	    byte[] buffer = new byte[8192];
//	    int bytesRead;
//	    while ((bytesRead = is.read(buffer)) > 0) {
//	        os.write(buffer, 0, bytesRead);
//	    }
//	    os.close();
//	    is.close();
//	}
	
	
	private static void assertEqual(InputStream is, File file) {
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
	    } catch (IOException e) {
	        Assert.fail(e.getMessage());
	    }
	}
	
	
}
