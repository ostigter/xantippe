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


package xantippe.filestore;


/**
 * Exception thrown by the FileStore class.
 * 
 * @author Oscar Stigter
 */
public class FileStoreException extends Exception {
    

    /** Serial version UID. */
    private static final long serialVersionUID = -1L;
    

    /* package */ FileStoreException(String message) {
        super(message);
    }


    /* package */ FileStoreException(String message, Throwable t) {
        super(message, t);
    }


}
