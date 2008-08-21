package xantippe;


import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;


/**
 * Document index based on a key value.
 * 
 * @author Oscar Stigter
 */
class IndexValue {
    

    /** Document ID's by key values. */
    private final Map<Object, Set<Integer>> documents;
    
    
    //------------------------------------------------------------------------
    //  Constructors
    //------------------------------------------------------------------------

    
    /* package */ IndexValue() {
        documents = new TreeMap<Object, Set<Integer>>();
    }
    
    
    //------------------------------------------------------------------------
    //  Public methods
    //------------------------------------------------------------------------
    
    
    public void indexDocument(int docId, Object value) {
        Set<Integer> docs = documents.get(value);
        if (docs == null) {
            docs = new TreeSet<Integer>();
            documents.put(value, docs);
        }
        docs.add(docId);
    }
    
    
    public Set<Integer> findDocuments(Object value) {
        Set<Integer> docs = new TreeSet<Integer>();
        
        Set<Integer> ids = documents.get(value);
        if (ids != null) {
            docs.addAll(ids);
        }
        
        return docs;
    }
    
    
    public int size() {
        return documents.size();
    }
    
    
    
    public Set<Object> getValues() {
        return documents.keySet();
    }
    
    
    public Set<Integer> getDocuments(Object value) {
        return documents.get(value);
    }
    
    
}
