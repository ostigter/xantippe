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
 * Supported media types for resources.
 * 
 * @author Oscar Stigter
 */
public enum MediaType {
    
    /** XML document. */
    XML("text/xml"),
    
    /** XML schema. */
    SCHEMA("text/xsd"),
    
    /** XQuery module. */
    XQUERY("application/xquery"),
    
    /** Plain text resource. */
    PLAIN_TEXT("text/plain"),
    
    /** Binary resource. */
    BINARY("octal/binary"),
    
    ;
    
    /** MIME content type. */
    private final String contentType;
    
    /**
     * Constructor.
     * 
     * @param contentType
     *            The content type.
     */
    MediaType(String contentType) {
        this.contentType = contentType;
    }
    
    /**
     * Returns the content type.
     * 
     * @return The content type.
     */
    public String getContentType() {
        return contentType;
    }

}
