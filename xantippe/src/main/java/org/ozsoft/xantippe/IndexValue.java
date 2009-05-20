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


import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
        documents = new HashMap<Object, Set<Integer>>();
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
