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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * Lock manager for synchronizing parallel database operations, using a
 * re-entrant read/write locking system.
 * 
 * @author Oscar Stigter
 */
class LockManager {
    
	/** Locks mapped by object ID. */
	private final Map<Integer, ReentrantReadWriteLock> locks;

	/**
	 * Constructor.
	 */
	public LockManager() {
        locks = new HashMap<Integer, ReentrantReadWriteLock>();
    }
	
	public synchronized void lockRead(Document doc) {
	    lockRead(doc.getId());
        Collection col = doc.getParent();
        if (col != null) {
            lockRead(col);
        }
	}
	
    public synchronized void unlockRead(Document doc) {
        unlockRead(doc.getId());
        Collection col = doc.getParent();
        if (col != null) {
            unlockRead(col);
        }
    }
    
	public synchronized void lockRead(Collection col) {
	    lockRead(col.getId());
	    Collection parent = col.getParent();
	    if (parent != null) {
	        lockRead(parent);
	    }
	}
    
    public synchronized void unlockRead(Collection col) {
        unlockRead(col.getId());
        Collection parent = col.getParent();
        if (parent != null) {
            unlockRead(parent);
        }
    }
    
    public synchronized void lockWrite(Document doc) {
        lockWrite(doc.getId());
        Collection col = doc.getParent();
        if (col != null) {
            lockRead(col);
        }
    }
    
    public synchronized void unlockWrite(Document doc) {
        unlockWrite(doc.getId());
        Collection col = doc.getParent();
        if (col != null) {
            unlockRead(col);
        }
    }
    
    public synchronized void lockWrite(Collection col) {
        lockWrite(col.getId());
        Collection parent = col.getParent();
        if (parent != null) {
            lockRead(parent);
        }
    }
    
    public synchronized void unlockWrite(Collection col) {
        unlockWrite(col.getId());
        Collection parent = col.getParent();
        if (parent != null) {
            unlockRead(parent);
        }
    }
    
	/**
	 * Locks a specific object for reading (a shared lock).
	 * 
	 * If another thread has a write lock on this object, the current thread
	 * will be blocked until that lock is released. 
	 * 
	 * @param  objectId  the object ID
	 */
	private synchronized void lockRead(int objectId) {
        ReadLock lock = getLock(objectId).readLock();
        while (true) {
            if (lock.tryLock()) {
                break;
            } else {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // Safe to ignore.
                }
            }
        }
    }

    /**
     * Locks a specific object for writing (an exclusive lock).
     * 
     * If any other thread has a lock on this object, the current thread will
     * be blocked until all locks are released. 
     * 
     * @param  objectId  the object ID
     */
    private void lockWrite(int objectId) {
        WriteLock lock = getLock(objectId).writeLock();
        while (true) {
            if (lock.tryLock()) {
                break;
            } else {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // Safe to ignore.
                }
            }
        }
    }

    /**
     * Releases a read lock on a specific object.
     * 
     * @param  objectId  the object ID
     * 
     * @throws  IllegalStateException
     *              if the current thread does not have any read lock for the
     *              specified object
     */
    private void unlockRead(int objectId) {
        ReentrantReadWriteLock lock = locks.get(objectId);
        if (lock != null) {
            lock.readLock().unlock();
            notify();
        } else {
            throw new IllegalStateException(
                    "Object not locked for reading: " + objectId);
        }
    }

    /**
     * Releases a write lock on a specific object.
     * 
     * @param  objectId  the object ID
     * 
     * @throws  IllegalStateException
     *              if the current thread does not have a write lock for the
     *              specified object
     */
    private void unlockWrite(int objectId) {
        ReentrantReadWriteLock lock = locks.get(objectId);
        if (lock != null) {
            lock.writeLock().unlock();
            notify();
        } else {
            throw new IllegalStateException(
                    "Object not locked for writing: " + objectId);
        }
    }

    /**
     * Returns a lock for a specific object ID.
     * 
     * If such a lock does not yet exist, it is created.
     * 
     * @param   objectId  the object ID
     * 
     * @return  the lock 
     */
    private ReentrantReadWriteLock getLock(int objectId) {
        ReentrantReadWriteLock lock = locks.get(objectId);
        if (lock == null) {
            lock = new ReentrantReadWriteLock();
            locks.put(objectId, lock);
        }
        return lock;
    }

}
