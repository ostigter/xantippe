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

import java.io.File;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Static class with various utility methods.
 *  
 * @author Oscar Stigter
 */
public abstract class Util {
    
    /** Config file for log4j. */
    private static final String LOG_CONFIG_FILE = "log4j.xml";
    
    /** Indicates whether log4j has been initialized. */
    private static boolean logInitialized = false; 
    
    /**
     * Configures log4j.
     */
    public static void initLog4j() {
        if (!logInitialized) {
            File file = new File(LOG_CONFIG_FILE);
            if (file.isFile() && file.canRead()) {
                DOMConfigurator.configure(file.getPath());
            } else {
                Logger rootLogger = Logger.getRootLogger();
                rootLogger.removeAllAppenders();
                rootLogger.addAppender(new ConsoleAppender(
                        new PatternLayout("%d %-5p %F:%M:%L - %m%n")));
                rootLogger.setLevel(Level.INFO);
            }
            
            logInitialized = true;
        }
    }
    
    /**
     * Deletes a file or directory (recursively).
     * 
     * @param  path  the file path
     */
    public static void deleteFile(String path) {
        deleteFile(new File(path));
    }

    /**
     * Deletes a file or directory (recursively).
     * 
     * @param  file  the file
     */
    public static void deleteFile(File file) {
        if (file.exists()) {
            if (file.isDirectory()) {
                for (File f : file.listFiles()) {
                    deleteFile(f);
                }
            }
            file.delete();
        }
    }
    
}
