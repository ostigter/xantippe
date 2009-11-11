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

/**
 * Document key used to retrieve documents using indexes.
 * Basically a simple name/value combination.
 * 
 * Implemented as an immutable class.
 * 
 * @author Oscar Stigter
 */
public class IndexKey {
    
    /** Key name. */
    private final String name;
    
    /** Key value. */
    private final Object value;
    
    /**
     * Constructor.
     * 
     * @param  name   the name
     * @param  value  the value
     */
    public IndexKey(String name, Object value) {
        this.name = name;
        this.value = value;
    }
    
    /**
     * Returns the name.
     * 
     * @return  the name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Returns the value.
     * 
     * @return  the value
     */
    public Object getValue() {
        return value;
    }
    
    /**
     * Returns the string representation of this key.
     * 
     * A key is represented by its name.
     * 
     * @return  the string representation of this key
     */
    @Override
    public String toString() {
        return String.format("{%s = %s}",  name, value);
    }
    
}
