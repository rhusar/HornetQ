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

package org.hornetq.tests.integration.cluster;

import java.io.IOException;

import org.hornetq.core.server.cluster.impl.LockFileImpl;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A LockFileImplTest
 *
 * @author jmesnil
 *
 *
 */
public class LockFileImplTest extends UnitTestCase
{

   // Constants -----------------------------------------------------
   
   /**
    * A ThreadExtension
    *
    * @author jmesnil
    *
    *
    */
   private final class Activation extends Thread
   {
      private LockFileImpl backupLock;
      private LockFileImpl liveLock;

      public void run() {
         backupLock = new LockFileImpl(RandomUtil.randomString(), System.getProperty("java.io.tmpdir"));
         try
         {
            backupLock.lock();
         }
         catch (IOException e)
         {
            e.printStackTrace();
         }

         liveLock = new LockFileImpl(liveLockFileName, System.getProperty("java.io.tmpdir"));
         try
         {
            liveLock.lock();
         }
         catch (IOException e)
         {
            e.printStackTrace();
         }
      }
      
      public void close() throws IOException
      {
         if (liveLock != null)
         {
            liveLock.unlock();
         }
         if (backupLock != null)
         {
            backupLock.unlock();
         }
      }
   }

   public static final String liveLockFileName = "liveLock";
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static void main(String[] args)
   {
      try
      {
         final LockFileImpl liveLock = new LockFileImpl(liveLockFileName, System.getProperty("java.io.tmpdir"));
         liveLock.lock();
         Thread.sleep(1000000);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }      
   }
   
   // 1. Run the class as a Java application to execute the main() in a separate VM
   // 2. Run this test
   public void _testInterrupt() throws Exception
   {
      Activation t = new Activation();
      t.start();
      
      System.out.println("sleep");
      Thread.sleep(5000);

      t.close();
      
      long timeout = 10000;
      long start = System.currentTimeMillis();
      while (t.isAlive() && System.currentTimeMillis() - start < timeout)
      {
         System.out.println("before interrupt");
         t.interrupt();
         System.out.println("after interrupt");
         
         Thread.sleep(1000);
      }

      assertFalse(t.isAlive());

      t.join();
      
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
