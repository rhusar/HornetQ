/*
 * Copyright 2010 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.server.cluster.impl;

import java.io.IOException;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.hornetq.core.server.cluster.LockFile;

/**
 * A FakeLockFile
 * 
 * A VM-wide exclusive lock on a file.
 * 
 * Advisory only.
 * 
 * Used for testing.
 *
 * @author Tim Fox
 *
 *
 */
public class FakeLockFile implements LockFile
{
   private final String fileName;

   private final String directory;
   
   private static Map<String, Lock> locks = new WeakHashMap<String, Lock>();
   
   private Lock lock;
   
   /**
    * @param fileName
    * @param directory
    */
   public FakeLockFile(final String fileName, final String directory)
   {
      this.fileName = fileName;
      
      this.directory = directory;
      
      synchronized (locks)
      {
         String key = directory + fileName;
         
         lock = locks.get(key);
         
         if (lock == null)
         {
            lock = new ReentrantLock(true);
            
            locks.put(key, lock);
         }
      }
   }
   
   public String getFileName()
   {
      return fileName;
   }

   public String getDirectory()
   {
      return directory;
   }

   public void lock() throws IOException
   {
      lock.lock();
   }

   public boolean unlock() throws IOException
   {
      lock.unlock();
      
      return true;
   }
}
