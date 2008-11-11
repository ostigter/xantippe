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


package xantippe;


import java.util.HashSet;
import java.util.Set;


/**
 * Lock manager for synchronizing parallel database operations.
 * 
 * The current implementation is very simple, but extremely fast.
 * 
 * @author Oscar Stigter
 */
class LockManager {
    
    
//    private static final Logger logger = Logger.getLogger(LockManager.class);

    private final Set<Integer> locks;
    
    private final Object lock = new Object();
    
    
    public LockManager() {
        locks = new HashSet<Integer>();
    }
    
    
    public boolean isLocked(int id) {
        return locks.contains(id);
    }
    
    
    public void lock(int id) {
//        logger.debug("Aquiring lock for ID " + id);
        
        synchronized (lock) {
            while (locks.contains(id)) {
                Thread.yield();
            }
        
            locks.add(id);

//            logger.debug("Locked ID " + id);
        }
    }
    
    
    public void unlock(int id) {
        locks.remove(id);
//        logger.debug("Unlocked ID " + id);
    }

}
