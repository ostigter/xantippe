package xantippe;


public enum ValidationMode {
	
	
	/** Required validation of documents. */
	ON,
	
	/** No document validation (disabled). */
	OFF,

	/** Validate documents if possible (i.e. document namespace present). */ 
	AUTO,
	
	/** Inherit from parent. */
	INHERIT,
	

}
