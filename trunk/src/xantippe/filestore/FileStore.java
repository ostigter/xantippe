package xantippe.filestore;


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

import org.apache.log4j.Logger;



/**
 * File system based database for storing documents.
 *
 * Documents are logically stored, retrieved, listed and deleted based on an
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
 * For performance reasons, this class offers no synchronization and is thus
 * NOT thread-safe.
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
    
    /** log4j logger. */
    private static final Logger logger = Logger.getLogger(FileStore.class);
    
    /** Buffer size. */
    private static final int BUFFER_SIZE = 8192;  // 8 kB
    
    /** Buffer for reading and writing files. */
    private final byte[] buffer = new byte[BUFFER_SIZE]; 

    /** Document entries mapped by the their ID. */
    private final Map<Integer, FileEntry> entries;
    
    /** Indicates whether the FileStore is running. */
    private boolean isRunning = false;
    
    /** Database directory. */
    private String dataDir;
    
    /** Data file with the document contents. */ 
    private RandomAccessFile dataFile;
    

    //------------------------------------------------------------------------
    //  Constructors
    //------------------------------------------------------------------------
    

    public FileStore() {
        this(DEFAULT_DATA_DIR);
    }
    

    public FileStore(String dataDir) {
        this.dataDir = dataDir;
        entries = new TreeMap<Integer, FileEntry>();
    }
    

    //------------------------------------------------------------------------
    //  Public methods
    //------------------------------------------------------------------------
    

    /**
     * Starts the <code>FileStore</code>.
     * 
     * @throws FileStoreException  if the FileStore could not be started
     */
    public void start() throws FileStoreException {
        if (!isRunning) {
            logger.debug("Starting");
            
            try {
                // Create data directory.
                File dir = new File(dataDir);
                if (!dir.exists()) {
                    dir.mkdirs();
                }
            } catch (Exception e) {
                String msg = String.format(
                        "Could not create data directory '%s': %s",
                        dataDir, e.getMessage());
                throw new FileStoreException(msg);
            }
            
            try {
                // Read index file.
                readIndexFile();
            } catch (IOException e) {
                String msg = String.format("Error reading file '%s': %s",
                        INDEX_FILE, e.getMessage());
                throw new FileStoreException(msg);
            }
            
            try {
                // Open data file.
                dataFile = new RandomAccessFile(
                        dataDir + '/' + DATA_FILE, "rw");
            } catch (IOException e) {
                String msg = String.format("Error openen data file '%s': %s",
                        DATA_FILE, e.getMessage());
                throw new FileStoreException(msg);
            }

            isRunning = true;
            
            logger.debug("Started");
        } else {
            throw new FileStoreException("FileStore already running");
        }
    }
    

    /**
     * Shuts down the <code>FileStore</code>.
     * 
     * @throws FileStoreException  If the FileStore could not be shut down
     */
    public void shutdown() throws FileStoreException {
        if (isRunning) {
            logger.debug("Shutting down");
            
            sync();
            
            try {
                dataFile.close();
            } catch (IOException e) {
                throw new FileStoreException("Error closing data file", e);
            } finally {
                isRunning = false;
            }
            
            entries.clear();
            
            logger.debug("Shut down");
        } else {
            throw new FileStoreException("FileStore not started");
        }
    }
    
    
    /**
     * Returns whether the <code>FileStore</code> is running.
     * 
     * @return  whether the <code>FileStore</code> is running
     */
    public boolean isRunning() {
        return isRunning;
    }
    

    /**
     * Returns the number of stored documents.
     * 
     * @return  the number of stored documents
     */
    public int size() {
        return entries.size();
    }
    

    /**
     * Stores a document based on a <code>File</code> object.
     * 
     * @param id  the document ID
     * @param file  the file with the document contents
     * 
     * @throws FileStoreException  if the document could not be stored
     */
    public void store(int id, File file) throws FileStoreException {
        if (isRunning) {
            logger.debug("Storing document with ID " + id);
            
            int length = (int) file.length();
            
            entries.remove(id);
            
            int offset = findFreePosition(length);
            logger.debug("Storing entry with offset " + offset
                    + " and length " + length + "...");
                
            FileEntry entry = new FileEntry(id);
            entry.setOffset(offset);
            entry.setLength(length);
            entries.put(id, entry);
            
            try {
                dataFile.seek(offset);
                InputStream is = new FileInputStream(file);
                int bytesRead;
                while ((bytesRead = is.read(buffer)) > 0) {
                    dataFile.write(buffer, 0, bytesRead);
                }
                is.close();
            } catch (IOException e) {
                entries.remove(id);
                String msg = "Could not store document with ID " + id;
                logger.error(msg, e);
                throw new FileStoreException(msg, e);
            }
        } else {
            throw new FileStoreException("FileStore not running");
        }
    }
    

    /**
     * Retrieves the content of a document.
     *  
     * @param id  the document ID
     * 
     * @return  the document content
     * 
     * @throws FileStoreException
     *             if the document content could not be retrieved
     */
    public InputStream retrieve(int id) throws FileStoreException {
        InputStream is = null;
        
        if (isRunning) {
            logger.debug("Retrieving entry with ID " + id);
            FileEntry entry = entries.get(id);
            if (entry != null) {
                try {
                    is = new RetrieveStream(
                            dataFile, entry.getOffset(), entry.getLength());
                } catch (IOException e) {
                    String msg = String.format(
                            "Error retrieving entry with ID %d: %s",
                            id, e.getMessage());
                    logger.error(msg);
                    throw new FileStoreException(msg, e);
                }
            } else {
                String msg = "Entry with ID " + id + " not found";
                logger.error(msg);
                throw new FileStoreException(msg);
            }
        } else {
            throw new FileStoreException("FileStore not running");
        }
        
        return is;
    }
    
    
    public int getLength(int id) {
        FileEntry entry = entries.get(id);
        if (entry != null) {
            return entry.getLength();
        } else {
            return -1;
        }
    }
    
    
    public void delete(int id) throws FileStoreException {
        if (isRunning) {
            FileEntry entry = entries.get(id);
            if (entry != null) {
                entries.remove(id);
                logger.debug("Deleted entry with ID " + id + " with offset "
                        + entry.getOffset() + " and length "
                        + entry.getLength());
            }
        } else {
            throw new FileStoreException("FileStore not running");
        }
    }
    

    /**
     * Deletes ALL files.
     */
    public void deleteAll() throws FileStoreException {
        if (isRunning) {
            entries.clear();
            
            sync();
            
            try {
                dataFile.setLength(0L);
            } catch (IOException e) {
                String msg = "Error clearing data file";
                logger.error(msg, e);
            }
            
            logger.debug("Deleted all entries.");
        } else {
            throw new FileStoreException("FileStore not running");
        }
    }
    

    /**
     * Writes any volatile meta-data to disk.
     */
    public void sync() throws FileStoreException {
        if (isRunning) {
            logger.debug("Sync");
            try {
                writeIndexFile();
            } catch (IOException e) {
                String msg = "Error sync'ing to disk: " + e.getMessage();
                logger.error(msg);
                throw new FileStoreException(msg);
            }
        } else {
            throw new FileStoreException("FileStore not running");
        }
    }
  

    /**
     * Logs a message showing the disk size usage.
     */
    public void printSizeInfo() {
        if (isRunning) {
            long stored = getStoredSpace();
            long used = getUsedSpace();
            long wasted = stored - used;
            double wastedPerc = 0.0;
            if (stored > 0) {
                wastedPerc = ((double) wasted / (double) stored) * 100;
            }
            logger.debug(String.format(Locale.US,
                    "Disk usage:  Size: %s, Used: %s, Wasted: %s (%.1f %%)",
                    diskSizeToString(stored), diskSizeToString(used),
                    diskSizeToString(wasted), wastedPerc));
        }
    }
    

    //------------------------------------------------------------------------
    //  Private methods
    //------------------------------------------------------------------------
    

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
        File file = new File(dataDir + '/' + INDEX_FILE);
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
        DataOutputStream dos = new DataOutputStream(
                new FileOutputStream(dataDir + '/' + INDEX_FILE));
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
            logger.error("Error retrieving data file length", e);
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
 

    //------------------------------------------------------------------------
    //  Inner classes
    //------------------------------------------------------------------------
    

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


} // FileStore
