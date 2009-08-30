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
 * Supported validation modes.
 * 
 * @author Oscar Stigter
 */
public enum ValidationMode {
    
    /** No document validation (disabled). */
    OFF,

    /** Validate each document; matching schema MUST be present. */
    ON,
    
    /** Validate documents only if they have a namespace and matching schema. */ 
    AUTO,
    
    /** Inherit setting from parent collection. */
    INHERIT,

}
