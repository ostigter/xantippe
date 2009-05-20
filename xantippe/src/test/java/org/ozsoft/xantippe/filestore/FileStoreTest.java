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

package org.ozsoft.xantippe.filestore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ozsoft.xantippe.Util;
import org.ozsoft.xantippe.filestore.FileStore;
import org.ozsoft.xantippe.filestore.FileStoreException;


/**
 * Test suite for the <code>FileStore</code> class.
 * 
 * @author Oscar Stigter
 */
public class FileStoreTest {


    private static final String RESOURCES_DIR = "src/test/resources";
    
    /** Temporary database directory. */ 
    private static final String DATA_DIR = RESOURCES_DIR + "/data.tmp";
    
    private static final String DB_DIR = RESOURCES_DIR + "/db";

    /** log4j logger. */
    private static final Logger logger = Logger.getLogger(FileStoreTest.class);

    
    //------------------------------------------------------------------------
    //  Setup and cleanup methods
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
    public void fileStore() {
        logger.debug("Test suite 'fileStore' started");
        
        File file;
        
        FileStore store = new FileStore();

        try {
            store.setDataDir(DATA_DIR);
            
            store.start();
            
            Assert.assertEquals(0, store.size());
            
            // Add file.
            file = new File(DB_DIR + "/data/foo/Foo-0001.xml");
            store.store(1, file);
            Assert.assertEquals(1, store.size());
            
            // Retrieve file.
            InputStream is = store.retrieve(1);
            Assert.assertNotNull(is);
            Assert.assertEquals(313, is.available());
            Assert.assertEquals('<', (char) is.read());
            Assert.assertEquals(312, is.available());
            Assert.assertEquals('?', (char) is.read());
            Assert.assertEquals(311, is.available());
            Assert.assertEquals('x', (char) is.read());
            Assert.assertEquals(310, is.available());
            Assert.assertEquals('m', (char) is.read());
            Assert.assertEquals(309, is.available());
            Assert.assertEquals('l', (char) is.read());
            Assert.assertEquals(308, is.available());
            byte[] buffer = new byte[308];
            int read = is.read(buffer, 0, 8);
            Assert.assertEquals(8, read);
            Assert.assertEquals(" version", new String(buffer, 0, read));
            read = is.read(buffer);
            Assert.assertEquals(300, read);
            is.close();
            
            // Add file.
            file = new File(DB_DIR + "/data/foo/Foo-0002.xml");
            store.store(2, file);
            Assert.assertEquals(2, store.size());
            assertEqual(store.retrieve(2), file);
            
            // Update file.
            store.store(2, file);
            Assert.assertEquals(2, store.size());
            assertEqual(store.retrieve(2), file);
            
            // Add file.
            file = new File(DB_DIR + "/data/bar/Bar-0001.xml");
            store.store(3, file);
            Assert.assertEquals(3, store.size());
            assertEqual(store.retrieve(3), file);
            
            // Delete file.
            store.delete(2);
            Assert.assertEquals(2, store.size());
            
            store.shutdown();
            
        } catch (FileStoreException e) {
            Assert.fail(e.getMessage());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
        
        logger.debug("Test suite 'fileStore' finished");
    }
    
    
    //------------------------------------------------------------------------
    //  Private methods
    //------------------------------------------------------------------------
    
    
    /**
     * Asserts that the contents of the specified byte stream is equal to that
     * of the specified file.
     * 
     *  @param  is    the byte stream
     *  @param  file  the file
     */
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
