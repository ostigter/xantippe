package xantippe;


/**
 * Definition of a document index.
 * 
 * @author Oscar Stigter
 */
public class Index implements Comparable<Index> {
    
    
    /** ID. */
    private int id;
    
    /** Name. */
    private String name;
    
    /** Path to the indexed XML element or attribute. */
    private String path;
    
    /** Type. */
    private IndexType type;
    
    
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


    //------------------------------------------------------------------------
    //  Interface implementation: Comparable
    //------------------------------------------------------------------------

    
    public int compareTo(Index index) {
        return name.compareTo(index.getName());
    }
    

    //------------------------------------------------------------------------
    //  Overriden methods: Object
    //------------------------------------------------------------------------

    
    @Override
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
