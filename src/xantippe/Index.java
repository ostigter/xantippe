package xantippe;


/**
 * Definition of a document index.
 * 
 * @author Oscar Stigter
 */
public class Index implements Comparable<Index> {
    
    
    /** ID. */
    private final int id;
    
    /** Name. */
    private final String name;
    
    /** Path to the indexed XML element or attribute. */
    private final String path;
    
    /** Type. */
    private final IndexType type;
    
    
    //------------------------------------------------------------------------
    //  Constructors
    //------------------------------------------------------------------------

    
    public Index(int id, String name, String path, IndexType type) {
        this.id = id;
        this.name = name;
        this.path = path;
        this.type = type;
    }


    //------------------------------------------------------------------------
    //  Public methods
    //------------------------------------------------------------------------

    
    public String getName() {
        return name;
    }


    public String getPath() {
        return path;
    }
    
    
    public IndexType getType() {
        return type;
    }


    public int compareTo(Index index) {
        return name.compareTo(index.getName());
    }
    

    @Override // Object
    public String toString() {
        return name;
    }


    //------------------------------------------------------------------------
    //  Package protected methods
    //------------------------------------------------------------------------

    
    /* package */ int getId() {
        return id;
    }
    
    
}
