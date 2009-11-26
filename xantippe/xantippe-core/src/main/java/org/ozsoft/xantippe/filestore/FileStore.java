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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * File system based database for storing documents.
 *
 * Documents are logically stored, retrieved and deleted based on an integer
 * ID.
 *
 * Document entries (the administration) are stored in a binary file
 * 'documents.dbx'. The document contents are stored in a binary file
 * 'contents.dbx'.
 *
 * All database files are stored in a configurable directory (by default
 * 'data'). If this directory does not exists, it will be created.
 *
 * The positioning algorithm for storing a document is simple; it inserts the
 * document at the first free position it fits in, or it appends the document
 * at the end.
 * 
 * For performance reasons, this class offers no synchronization or locking,
 * and is thus NOT thread-safe.
 *
 * @author Oscar Stigter
 */
public class FileStore {
    
    /** Default database directory. */
    private static final String DEFAULT_DATA_DIR = "data";
    
    /** File with the document entries (FAT). */
    private static final String INDEX_FILE = "documents.dbx";
    
    /** File with the document contents. */
    private static final String DATA_FILE = "contents.dbx";
    
    /** Buffer size. */
    private static final int BUFFER_SIZE = 8192;  // 8 kB
    
    /** Log */
    private static final Log LOG = LogFactory.getLog(FileStore.class);
    
    /** Document entries mapped by the their ID. */
    private final Map<Integer, FileEntry> entries;
    
    /** Indicates whether the FileStore is running. */
    private boolean isRunning = false;
    
    /** Data directory. */
    private File dataDir = new File(DEFAULT_DATA_DIR);
    
    /** Data file with the document contents. */ 
    private RandomAccessFile dataFile;
    
    /**
     * Constructor.
     */
    public FileStore() {
        entries = new TreeMap<Integer, FileEntry>();
    }
    
    /**
     * Returns the data directory.
     * 
     * @return  the data directory
     */
    public String getDataDir() {
        return dataDir.getAbsolutePath();
    }

    /**
     * Sets the data directory.
     * 
     * The data directory may only be set if the database is not running.
     * 
     * @param path  the data directory
     * 
     * @throws IllegalArgumentException
     *             if the path is null or empty
     * @throws IllegalStateException
     *             if the FileStore is running
     */
    public void setDataDir(String path) {
        if (path == null || path.length() == 0) {
            throw new IllegalArgumentException("Null or empty path");
        }

        if (isRunning) {
            throw new IllegalStateException("FileStore is running");
        }

        dataDir = new File(path);
    }

    /**
     * Starts the FileStore.
     *
     * @throws  IllegalStateException
     *              if the database is already running
     * @throws  FileStoreException
     *             if the data directory could not be created, the index file
     *             could not be read, or the data file could not be opened
     */
    public void start() throws FileStoreException {
        if (isRunning) {
            throw new IllegalStateException("Database already running");
        }

        LOG.debug("Starting");
            
        // Create data directory.
        if (!dataDir.exists()) {
            if (!dataDir.mkdirs()) {
                String msg = "Could not create data directory: " + dataDir;
                LOG.error(msg);
                throw new FileStoreException(msg);
            }
        }
        
        try {
            // Read index file.
            readIndexFile();
        } catch (IOException e) {
            String msg = String.format("Error reading file '%s': %s",
                    INDEX_FILE, e.getMessage());
            throw new FileStoreException(msg, e);
        }
        
        try {
            // Open data file.
            dataFile = new RandomAccessFile(
                    new File(dataDir, DATA_FILE), "rw");
        } catch (IOException e) {
            String msg = String.format("Error opening data file '%s': %s",
                    DATA_FILE, e.getMessage());
            throw new FileStoreException(msg, e);
        }

        isRunning = true;
        
        LOG.debug("Started");
    }
    
    /**
     * Shuts down the FileStore.
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     * @throws  FileStoreException
     *              if the index file could not be written, or data file could
     *              not be closed  
     */
    public void shutdown() throws FileStoreException {
        checkIsRunning();
        
        LOG.debug("Shutting down");
        
        sync();
        
        try {
            dataFile.close();
            
        } catch (IOException e) {
            String msg = "Error closing data file: " + e.getMessage();
            LOG.error(msg, e);
            throw new FileStoreException(msg, e);
            
        } finally {
            isRunning = false;
        }
        
        entries.clear();
        
        LOG.debug("Shut down");
    }
    
    /**
     * Returns whether the FileStore is running.
     * 
     * @return  true if the FileStore is running, otherwise false
     */
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * Returns the number of stored documents.
     * 
     * @return  the number of stored documents
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     */
    public int size() {
        return entries.size();
    }
    
    /**
     * Indicates whether a stored document with the specified ID exists.
     *  
     * @param   id  the document ID
     * 
     * @return  true  if a stored document with the specified ID exists,
     *                otherwise false
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     */
    public boolean exists(int id) {
        checkIsRunning();
        return entries.containsKey(id);
    }
    
    /**
     * Creates a new document.
     * 
     * If a document with the specified ID already exists, 
     * 
     * @param   id  the document ID
     * 
     * @return  the new document
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     * @throws  FileStoreException
     *              if a document with the specified ID already exists
     */
    public FileEntry create(int id) throws FileStoreException {
        checkIsRunning();
        
        if (exists(id)) {
            throw new FileStoreException(
                    String.format("Could not create document; ID %d not unique", id));
        }
            
        FileEntry entry = new FileEntry(id);
        entry.setOffset(0);
        entry.setLength(0);
        entries.put(id, entry);
        
        return entry;
    }
    
    /**
     * Stores a document based on a file.
     * 
     * @param   id    the document ID
     * @param   file  the file with the document's content
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     * @throws  FileStoreException
     *              if the file could not be read, or the document content
     *              could not be written
     */
    public void store(int id, File file) throws FileStoreException {
        checkIsRunning();
        
        int length = (int) file.length();
        
        // Delete (overwrite) any previous document with the same ID.
        entries.remove(id);
        
        int offset = findFreePosition(length);
            
        FileEntry entry = create(id);

        entry.setOffset(offset);
        entry.setLength(length);
        
        try {
            dataFile.seek(offset);
            InputStream is = new FileInputStream(file);
            byte buffer[] = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) > 0) {
                dataFile.write(buffer, 0, bytesRead);
            }
            is.close();
        } catch (IOException e) {
            entries.remove(id);
            String msg = String.format("Could not store document with ID %d", id);
            LOG.error(msg, e);
            throw new FileStoreException(msg, e);
        }
    }

    /**
     * Retrieves the content of a document.
     *  
     * @param   id  the document ID
     * 
     * @return  the document content
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     * @throws  FileStoreException
     *              if the document could not be found, or the content could
     *              not be read
     */
    public InputStream retrieve(int id) throws FileStoreException {
        checkIsRunning();
        
        InputStream is = null;
        
        FileEntry entry = entries.get(id);
        if (entry != null) {
            try {
                is = new RetrieveStream(
                        dataFile, entry.getOffset(), entry.getLength());
            } catch (IOException e) {
                String msg = String.format(
                        "Error retrieving document with ID %d: %s",
                        id, e.getMessage());
                LOG.error(msg, e);
                throw new FileStoreException(msg, e);
            }
        } else {
            String msg = "Document with ID " + id + " not found";
            LOG.error(msg);
            throw new FileStoreException(msg);
        }
        
        return is;
    }
    
    /**
     * Returns the length of a document.
     * 
     * When the specified document does not exist, 0 is returned.
     *  
     * @param  id  the document ID
     * 
     * @return  the document length in bytes
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     */
    public int getLength(int id) {
        checkIsRunning();
        
        int length = 0;
        
        FileEntry entry = entries.get(id);
        if (entry != null) {
            length = entry.getLength();
        }
        
        return length;
    }
    
    /**
     * Deletes a document.
     * 
     * @param   id  the document ID
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     */
    public void delete(int id) throws FileStoreException {
        checkIsRunning();
        
        FileEntry entry = entries.get(id);
        if (entry != null) {
            entries.remove(id);
        }
    }
    
    /**
     * Writes any volatile meta-data to disk.
     */
    public void sync() throws FileStoreException {
        if (isRunning) {
            try {
                writeIndexFile();
            } catch (IOException e) {
                String msg = "Error sync'ing to disk";
                LOG.error(msg, e);
                throw new FileStoreException(msg, e);
            }
        }
    }

    /**
     * Logs a message showing the disk size usage.
     * 
     * @throws  IllegalStateException
     *              if the database is not running
     */
    public void printSizeInfo() {
        checkIsRunning();
        
        long stored = getStoredSpace();
        long used = getUsedSpace();
        long wasted = stored - used;
        double wastedPerc = 0.0;
        if (stored > 0) {
            wastedPerc = ((double) wasted / (double) stored) * 100;
        }
        LOG.debug(String.format(Locale.US,
                "Disk usage:  Size: %s, Used: %s, Wasted: %s (%.1f %%)",
                diskSizeToString(stored), diskSizeToString(used),
                diskSizeToString(wasted), wastedPerc));
    }

    /**
     * Checks that the database is running.
     * 
     * @throws  IllegalStateException  if the database is not running.
     */
    private void checkIsRunning() {
        if (!isRunning) {
            throw new IllegalStateException("FileStore not running");
        }
    }

    /**
     * Returns the offset of the first free position in the data file that
     * would fit a file with the specified length.
     * 
     * @param  length  the file length in bytes
     * 
     * @return  the offset  
     */
    private int findFreePosition(int length) {
        int offset = 0;
        for (FileEntry entry : entries.values()) {
            // Look for free space between entries.
            long free = entry.getOffset() - offset;
            if (free >= length) {
                // Found a suitable spot!
                break;
            } else {
                // Proceed to next entry.
                offset = entry.getOffset() + entry.getLength();
            }
        }
        return offset;
    }
    
    /**
     * Reads the index file.
     * 
     * @throws  IOException  if the file could not be read
     */
    private void readIndexFile() throws IOException {
        entries.clear();
        File file = new File(dataDir, INDEX_FILE);
        if (file.exists()) {
            DataInputStream dis =
                    new DataInputStream(new FileInputStream(file));
            int noOfEntries = dis.readInt();
            for (int i = 0; i < noOfEntries; i++) {
                int id = dis.readInt();
                int offset = dis.readInt();
                int length = dis.readInt();
                FileEntry entry = new FileEntry(id);
                entry.setOffset(offset);
                entry.setLength(length);
                entries.put(id, entry);
            }
            dis.close();
        }
    }
    
    /**
     * Writes the index file.
     * 
     * @throws  IOException  if the file could not be written
     */
    private void writeIndexFile() throws IOException {
        File file = new File(dataDir, INDEX_FILE);
        DataOutputStream dos =
                new DataOutputStream(new FileOutputStream(file));
        dos.writeInt(entries.size());
        for (FileEntry entry : entries.values()) {
            dos.writeInt(entry.getId());
            dos.writeInt(entry.getOffset());
            dos.writeInt(entry.getLength());
        }
        dos.close();
    }

    /**
     * Returns the size of the data file actually stored on disk.
     * 
     * @return  the size of the data file
     */
    private long getStoredSpace() {
        long size = 0L;
        try {
            size = dataFile.length();
        } catch (IOException e) {
            String msg =
                    "Error retrieving data file length: " + e.getMessage();
            LOG.error(msg, e);
        }
        return size;
    }
    
    /**
     * Returns the net used disk space for storing the documents without any
     * fragmentation.
     * 
     * @return  the net used disk space 
     */
    private long getUsedSpace() {
        long size = 0L;
        for (FileEntry entry : entries.values()) {
            size += entry.getLength();
        }
        return size;
    }
    
    /**
     * Returns a human-friendly representation of a file size.
     *  
     * @param size  the file size in bytes
     * 
     * @return  the human-friendly representation of the file size
     */
    private static String diskSizeToString(long size) {
        String s = null;
        if (size >= 1073741824L) {
            s = String.format(Locale.US, "%.2f GB", size / 1073741824.0);
        } else if (size >= 1048576L) {
            s = String.format(Locale.US, "%.2f MB", size / 1048576.0);
        } else if (size >= 1024L) {
            s = String.format(Locale.US, "%.2f kB", size / 1024.0);
        } else {
            s = String.format(Locale.US, "%d bytes", size);
        }
        return s;
    }
 
    /**
     * Administrative entry of a stored file.
     * 
     * Each entry has a name, offset and length.
     * 
     * @author Oscar Stigter
     */
    private static class FileEntry implements Comparable<FileEntry> {
        
        private int id;
        
        private int offset;
        
        private int length;

        public FileEntry(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public int getOffset() {
            return offset;
        }

        public void setOffset(int offset) {
            this.offset = offset;
        }

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }

        @Override
        public String toString() {
            return "{'" + id + "', " + offset + ", " + length + "}";
        }

        @Override // Object
        public int hashCode() {
            return id;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof FileEntry) {
                FileEntry entry = (FileEntry) obj;
                return entry.getId() == id;
            } else {
                return false;
            }
        }

        public int compareTo(FileEntry entry) {
            int otherOffset = entry.getOffset();
            if (offset > otherOffset) {
                return 1;
            } else if (offset < otherOffset) {
                return -1;
            } else {
                return 0;
            }
        }

    } // FileEntry

}
