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

import java.io.File;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test suite for the lock manager.
 * 
 * @author Oscar Stigter
 */
public class LockManagerTest {

    private static final int NO_OF_THREADS = 100;
    
    private static final int NO_OF_THREAD_ITERATIONS = 1000;
    
    private static final String DB_DIR = "src/test/resources/db";
    
    private static final String DATA_DIR = "target/data.tmp";
    
    private static final Logger LOG =
            Logger.getLogger(LockManagerTest.class);
    
    private static DatabaseImpl database;

    
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
    public void locking() {
        LOG.debug("Test suite 'locking' started");
        
        try {
            LockManager lockManager = database.getLockManager();
            
            database.start();
            
            Collection root = database.getRootCollection();
            root.setCompressionMode(CompressionMode.NONE);
            root.setValidationMode(ValidationMode.OFF);
            Collection col = root.createCollection("foo");
            File file = new File(DB_DIR + "/data/foo/Foo-0001.xml");
            Document doc = col.createDocument(file.getName());
            doc.setContent(file);
            
            LockingThread[] clients = new LockingThread[NO_OF_THREADS];
            for (int i = 0; i < NO_OF_THREADS; i++) {
                clients[i] = new LockingThread(i, lockManager, col, doc);
            }
            for (LockingThread client : clients) {
                client.start();
            }
            for (LockingThread client : clients) {
                client.join();
                Assert.assertFalse("Error encountered in thread", client.hasErrors());
            }
            
            database.shutdown();
            
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        
        LOG.debug("Test suite 'locking' finished");
    }
    
    
    //------------------------------------------------------------------------
    //  Private classes
    //------------------------------------------------------------------------


    /**
     * Thread simulating an action requiring locking.
     */
    private class LockingThread extends Thread {
        
        private final int id;
        
        private final LockManager lockManager;
        
        private final Collection col;
        
        private final Document doc;
        
        private boolean hasErrors = false;
        
        public LockingThread(int id, LockManager lockManager, Collection col, Document doc) {
            this.id = id;
            this.col = col;
            this.doc = doc;
            this.lockManager = lockManager;
        }
        
        @Override
        public void run() {
            try {
                for (int i = 0; i < NO_OF_THREAD_ITERATIONS; i++) {
                    lockManager.lockRead(col);
                    lockManager.unlockRead(col);
                    lockManager.lockWrite(col);
                    lockManager.unlockWrite(col);
                    lockManager.lockRead(doc);
                    lockManager.unlockRead(doc);
                    //FIXME: Fix deadlock with document write locks.
//                    lockManager.lockWrite(doc);
//                    lockManager.unlockWrite(doc);
                }
            } catch (Exception e) {
                String msg = String.format(
                        "Exception in thread %d: %s", id, e.getMessage());
                LOG.error(msg, e);
                hasErrors = true;
            }
        }
        
        public boolean hasErrors() {
            return hasErrors;
        }
        
    } // LockingThread
    
}
