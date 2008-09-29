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
