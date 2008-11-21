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


import java.util.HashMap;
import java.util.Map;


/**
 * Lock manager for synchronizing parallel database operations.
 * 
 * The current implementation is very simple, but extremely fast.
 * 
 * @author Oscar Stigter
 */
class LockManager {
    
    
//    /** log4j logger. */
//    private static final Logger logger = Logger.getLogger(LockManager.class);

	/** Exclusive locks. */
	private final Map<Integer, String> exclusiveLocks;


    public LockManager() {
        exclusiveLocks = new HashMap<Integer, String>();
    }


    public boolean isLocked(int id) {
    	return exclusiveLocks.containsKey(id);
    }


    public synchronized void lock(int id) {
    	String clientId = Thread.currentThread().getName();
    	if (exclusiveLocks.containsKey(id)) {
    		String ownerId = exclusiveLocks.get(id);
    		if (!ownerId.equals(clientId)) {
    			// Someone else has a lock; wait for unlock.
    			while (exclusiveLocks.containsKey(id)) {
    				try {
    					wait();
    				} catch (InterruptedException e) {
    					// Safe to ignore.
    				}
    			}
    		}
    	}
		exclusiveLocks.put(id, clientId);
    }


    public synchronized void unlock(int id) {
        exclusiveLocks.remove(id);
        notifyAll();
    }


}
