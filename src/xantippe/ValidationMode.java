package xantippe;


public enum ValidationMode {
    
    
    /** No document validation (disabled). */
    OFF,

    /** Validate each document; matching schema must be present. */
    ON,
    
    /** Validate documents if it has a namespace and a matching schema. */ 
    AUTO,
    
    /** Inherit setting from parent collection. */
    INHERIT,
    

}
