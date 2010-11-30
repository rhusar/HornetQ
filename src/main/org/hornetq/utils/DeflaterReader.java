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

package org.hornetq.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Deflater;

/**
 * A DeflaterReader
 * The reader takes an inputstream and compress it.
 * Not for concurrent use.

 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public class DeflaterReader
{
   private Deflater deflater = new Deflater();
   private boolean isFinished = false;
   private boolean compressDone = false;

   private InputStream input;
   
   public DeflaterReader(InputStream inData)
   {
      input = inData;
   }
   
   public int read(byte[] buffer) throws IOException
   {
      return read(buffer, 0, buffer.length);
   }
   
   /**
    * Try to fill the buffer with compressed bytes. Except the last effective read,
    * this method always returns with a full buffer of compressed data.
    * 
    * @param buffer the buffer to fill compressed bytes
    * @return the number of bytes really filled, -1 indicates end.
    * @throws IOException 
    */
   public int read(byte[] buffer, int offset, int len) throws IOException
   {
      if (compressDone)
      {
         return -1;
      }
      
      //buffer for reading input stream
      byte[] readBuffer = new byte[2 * len];

      int n = 0;
      int read = 0;

      while (len > 0)
      {
         n = deflater.deflate(buffer, offset, len);
         if (n == 0)
         {
            if (isFinished)
            {
               deflater.end();
               compressDone = true;
               break;
            }
            else if (deflater.needsInput())
            {
               System.err.println("need input so read input");
               // read some data from inputstream
               int m = input.read(readBuffer);
               System.err.println("original data read: " + m);
               if (m == -1)
               {
                  System.err.println("no more original data, finish deflater, now offset " + offset + " len " + len);
                  
                  deflater.finish();
                  isFinished = true;
               }
               else
               {
                  deflater.setInput(readBuffer, 0, m);
               }
            }
            else
            {
               deflater.finish();
               isFinished = true;
            }
         }
         else
         {
            read += n;
            offset += n;
            len -= n;
         }
         
      }
      return read;
   }
   
   public static void main(String[] args) throws IOException
   {
      String inputString = "blahblahblah??blahblahblahblahblah??blablahblah??blablahblah??bla";
      byte[] input = inputString.getBytes("UTF-8");

      ByteArrayInputStream inputStream = new ByteArrayInputStream(input);
      
      DeflaterReader reader = new DeflaterReader(inputStream);
      
      byte[] buffer = new byte[7];
      
      int n = reader.read(buffer);
      
      System.err.println("first read: " + n);
      
      while (n != -1)
      {
         System.err.println("==>read n " + n + " values: " + getBytesString(buffer));
         n = reader.read(buffer);
      }
      
      System.err.println("compressed.");
      
      System.err.println("now verify");
      
      byte[] output = new byte[30];
      Deflater compresser = new Deflater();
      compresser.setInput(input);
      compresser.finish();
      int compressedDataLength = compresser.deflate(output);
      System.err.println("compress len: " + compressedDataLength);
      System.err.println("commpress data: " + getBytesString(output));

   }
   
   static String getBytesString(byte[] array)
   {
      StringBuffer bf = new StringBuffer();
      for (byte b : array)
      {
         int val = b & 0xFF;
         bf.append(val + " ");
      }
      return bf.toString();
   }
   
}
