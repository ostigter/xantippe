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


public enum CompressionMode {
    
    
    /** No compression. */
    NONE,
    
    /** Compress with DEFLATE (zlib) method (faster). */
    DEFLATE,
    
//    /** Compress with LZMA (7-Zip) method (better compression). */
//    LZMA,
    
    /** Inherit from parent collection. */
    INHERIT,
    

}
