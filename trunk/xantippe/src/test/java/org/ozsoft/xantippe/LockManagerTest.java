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


import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ozsoft.xantippe.Util;


/**
 * Test suite for the lock manager.
 * 
 * @author Oscar Stigter
 */
public class LockManagerTest {


    private static final int NO_OF_THREADS = 100;
    
    private static final int NO_OF_THREAD_ITERATIONS = 1000;
    
    private static final Logger logger =
            Logger.getLogger(LockManagerTest.class);
    
    private final LockManager lockManager = new LockManager();


    //------------------------------------------------------------------------
    //  Setup & cleanup methods
    //------------------------------------------------------------------------


    @BeforeClass
    public static void beforeClass() {
        Util.initLog4j();
    }
    

    //------------------------------------------------------------------------
    //  Tests
    //------------------------------------------------------------------------
    
    
    @Test
    public void locking() {
        logger.debug("Test suite 'locking' started");
        
        try {
            LockingThread[] clients = new LockingThread[NO_OF_THREADS];
            for (int i = 0; i < NO_OF_THREADS; i++) {
                clients[i] = new LockingThread(i);
            }
            for (LockingThread client : clients) {
                client.start();
            }
            for (LockingThread client : clients) {
                client.join();
                Assert.assertFalse("Error encountered in thread", client.hasErrors());
            }
            
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        
        logger.debug("Test suite 'locking' finished");
    }
    
    
    //------------------------------------------------------------------------
    //  Private classes
    //------------------------------------------------------------------------


    /**
     * Thread simulating an action requiring locking.
     */
    private class LockingThread extends Thread {
        
        
        private final int id;
        
        private boolean hasErrors = false;
        
        
        public LockingThread(int id) {
            this.id = id;
        }
        
        
        @Override // Thread
        public void run() {
            int objectId = 123;
            try {
                for (int i = 0; i < NO_OF_THREAD_ITERATIONS; i++) {
                    lockManager.lockRead(objectId);
                    lockManager.unlockRead(objectId);
                    lockManager.lockWrite(objectId);
                    lockManager.unlockWrite(objectId);
                }
            } catch (Exception e) {
                String msg = String.format(
                        "Exception in thread %d: %s", id, e.getMessage());
                logger.error(msg, e);
                hasErrors = true;
            }
        }
        
        
        public boolean hasErrors() {
            return hasErrors;
        }
        
        
    } // LockingThread class
    
    
} // LockManagerTest class
