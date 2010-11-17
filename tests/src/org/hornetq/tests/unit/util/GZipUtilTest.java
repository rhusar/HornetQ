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

package org.hornetq.tests.unit.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.hornetq.core.logging.Logger;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.GZipUtil;
import org.hornetq.utils.GZipUtil.GZipPipe;

/**
 * A GZipUtilTest
 *
 * @author Howard Gao
 *
 *
 */
public class GZipUtilTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(GZipUtilTest.class);

   //create a 10M file, zip it into another file
   //then unzip it and compare the result with original file
   public void testZipFunction() throws Exception
   {
      this.recreateDirectory(this.getTestDir());
      
      File originalFile = new File(this.getTestDir(), "gzipUtilTest_file.txt");
      File zippedFile = new File(this.getTestDir(), "gzipUtilTest_file.zip");
      
      FileOutputStream originalOut = new FileOutputStream(originalFile);
      FileOutputStream zippedOut = new FileOutputStream(zippedFile);
      
      //now create the file
      Random r = new Random();
      final int size = 1024 * 10;
      byte[] writeBuffer = new byte[1024];
      
      for (int i = 0; i < size; i++)
      {
         int b = r.nextInt(256);
         for (int j = 0; j < 1024; j++)
         {
            writeBuffer[j] = (byte)b;
         }
         originalOut.write(writeBuffer);       
      }
      originalOut.close();
      
      //now zip it
      GZipPipe pipe = new GZipPipe(new FileInputStream(originalFile), 2048);
      byte[] buffer = new byte[2048];
      
      int n = pipe.read(buffer);
      while (n != -1)
      {
         if (n > 0)
         {
            zippedOut.write(buffer, 0, n);
         }
         n = pipe.read(buffer);
      }
      zippedOut.close();

      //now unzip it and compare
      log.debug("zipped file Size: " + zippedFile.length());
      GZIPInputStream zippedInput = new GZIPInputStream(new FileInputStream(zippedFile));
      FileInputStream originalInput = new FileInputStream(originalFile);
      
      ArrayList<Integer> fromZip = new ArrayList<Integer>();
      ArrayList<Integer> original = new ArrayList<Integer>();
      
      byte[] readBuffer = new byte[2048];
      int count = zippedInput.read(readBuffer);
      
      while (count != -1)
      {
         for (int i = 0; i < count; i++)
         {
            fromZip.add(readBuffer[i] & 0xFF);
         }
         count = zippedInput.read(readBuffer);
      }
      zippedInput.close();

      count = originalInput.read(readBuffer);
      while (count != -1)
      {
         for (int i = 0; i < count; i++)
         {
            original.add(readBuffer[i] & 0xFF);
         }
         count = originalInput.read(readBuffer);
      }      
      originalInput.close();
      
      log.debug("fromZip: " + fromZip.size());
      compareByteArray(fromZip, original);
      
      originalFile.delete();
      zippedFile.delete();
   }
   
   //create a 10M file, zip it into another file
   //load it into an input stream and feed to the 
   //GZipOutput. Then compare the result
   public void testUnzipFunction() throws Exception
   {
      this.recreateDirectory(this.getTestDir());
      
      File originalFile = new File(this.getTestDir(), "gzipUtilTest_file1.txt");
      File zippedFile = new File(this.getTestDir(), "gzipUtilTest_file1.zip");
      File unzippedFile = new File(this.getTestDir(), "gzipUtilTest_unzipped.txt");
      
      FileOutputStream originalOut = new FileOutputStream(originalFile);
      FileOutputStream zippedOut = new FileOutputStream(zippedFile);
      
      //now create the file
      Random r = new Random();
      final int size = 1024 * 10;
      byte[] writeBuffer = new byte[1024];
      
      for (int i = 0; i < size; i++)
      {
         int b = r.nextInt(256);
         for (int j = 0; j < 1024; j++)
         {
            writeBuffer[j] = (byte)b;
         }
         originalOut.write(writeBuffer);       
      }
      originalOut.close();
      
      log.info("file created.");
      
      //now zip it
      GZIPOutputStream gzipOut = new GZIPOutputStream(zippedOut);
      FileInputStream originalIn = new FileInputStream(originalFile);
      
      byte[] buffer = new byte[2048];
      
      int n = originalIn.read(buffer);
      while (n != -1)
      {
         if (n > 0)
         {
            gzipOut.write(buffer, 0, n);
         }
         n = originalIn.read(buffer);
      }
      gzipOut.close();

      log.info("file zipped.");

      //get a zipped input stream
      FileInputStream zippedInput = new FileInputStream(zippedFile);
      
      FileOutputStream unzipOut = new FileOutputStream(unzippedFile);
      
      OutputStream newOut = GZipUtil.createZipOutputStream(unzipOut);
      
      n = zippedInput.read(buffer);
      while (n != -1)
      {
         if (n > 0)
         {
            newOut.write(buffer, 0, n);
         }
         n = zippedInput.read(buffer);
      }
      newOut.close();
      
      log.info("file unzipped");
      
      //compare original and unzipped
      FileInputStream originalInput = new FileInputStream(originalFile);
      FileInputStream unzippedInput = new FileInputStream(unzippedFile);
      
      ArrayList<Integer> fromZip = new ArrayList<Integer>();
      ArrayList<Integer> original = new ArrayList<Integer>();
      
      byte[] readBuffer = new byte[2048];
      int count = originalInput.read(readBuffer);
      
      while (count != -1)
      {
         for (int i = 0; i < count; i++)
         {
            original.add(readBuffer[i] & 0xFF);
         }
         count = originalInput.read(readBuffer);
      }
      originalInput.close();

      count = unzippedInput.read(readBuffer);
      
      while (count != -1)
      {
         for (int i = 0; i < count; i++)
         {
            fromZip.add(readBuffer[i] & 0xFF);
         }
         count = unzippedInput.read(readBuffer);
      }
      unzippedInput.close();

      compareByteArray(fromZip, original);
      
      originalFile.delete();
      zippedFile.delete();
      unzippedFile.delete();
   }
   
   private void compareByteArray(ArrayList<Integer> b1, ArrayList<Integer> b2)
   {
      assertEquals(b1.size(), b2.size());
      
      for (int i = 0; i < b1.size(); i++)
      {
         assertEquals(b1.get(i), b2.get(i));
      }
   }
}
