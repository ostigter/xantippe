package xantippe;


public enum ValidationMode {
	
	
	/** Validate each document; matching schema must be present. */
	ON,
	
	/** No document validation (disabled). */
	OFF,

	/** Validate documents if matching schema is present. */ 
	AUTO,
	
	/** Inherit setting from parent collection. */
	DEFAULT,
	

}
