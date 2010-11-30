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
import java.util.ArrayList;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * An InflaterReader
 * It takes an compressed input stream and decompressed it as it is being read.
 * Not for concurrent use.
 *
 */
public class InflaterReader extends InputStream
{
   private Inflater inflater = new Inflater();
   
   private InputStream input;
   
   private byte[] readBuffer;
   private int pointer;
   private int length;
   
   public InflaterReader(InputStream input)
   {
      this(input, 1024);
   }
   
   public InflaterReader(InputStream input, int bufferSize)
   {
      this.input = input;
      this.readBuffer = new byte[bufferSize];
      this.pointer = -1;
   }
   
   public static void log(String str)
   {
      System.out.println(str);
   }
   
   public int read() throws IOException
   {
      log("in read");
      
      if (pointer == -1)
      {
         log("pointer is -1");
         
         try
         {
            log("need to decompress more bytes");
            length = doRead(readBuffer, 0, readBuffer.length);
            log("bytes decompressed:" + length);
            if (length == 0)
            {
               log("zero byte got, ending");
               return -1;
            }
            log("reset pointer to zero");
            pointer = 0;
         }
         catch (DataFormatException e)
         {
            throw new IOException(e);
         }
      }
      
      log("reading byte at " + pointer);
      int value = readBuffer[pointer] & 0xFF;
      pointer++;
      if (pointer == length)
      {
         log("buffer all read, set pointer to -1");
         pointer = -1;
      }
      
      log("byte got: " + value);
      return value;
   }
   
   /*
    * feed inflater more bytes in order to get some
    * decompressed output.
    * returns number of bytes actually got
    */
   private int doRead(byte[] buf, int offset, int len) throws DataFormatException, IOException
   {
      int read = 0;
      int n = 0;
      byte[] inputBuffer = new byte[len];
      
      while (len > 0)
      {
         n = inflater.inflate(buf, offset, len);
         if (n == 0)
         {
            if (inflater.finished())
            {
               break;
            }
            else if (inflater.needsInput())
            {
               //feeding
               int m = input.read(inputBuffer);
               
               if (m == -1)
               {
                  //it shouldn't be here, throw exception
                  throw new DataFormatException("Input is over while inflater still expecting data");
               }
               else
               {
                  //feed the data in
                  inflater.setInput(inputBuffer);
                  n = inflater.inflate(buf, offset, len);
                  if (n > 0)
                  {
                     read += n;
                     offset += n;
                     len -= n;
                  }
               }
            }
            else
            {
               //it shouldn't be here, throw
               throw new DataFormatException("Inflater is neither finished nor needing input.");
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
      byte[] output = new byte[30];
      Deflater compresser = new Deflater();
      compresser.setInput(input);
      compresser.finish();
      int compressedDataLength = compresser.deflate(output);
      System.err.println("compress len: " + compressedDataLength);

      byte[] zipBytes = new byte[compressedDataLength];
      
      System.arraycopy(output, 0, zipBytes, 0, compressedDataLength);
      ByteArrayInputStream byteInput = new ByteArrayInputStream(zipBytes);
      
      InflaterReader inflater = new InflaterReader(byteInput);
      ArrayList<Integer> holder = new ArrayList<Integer>();
      int read = inflater.read();
      
      while (read != -1)
      {
         holder.add(read);
         read = inflater.read();
      }
      
      byte[] result = new byte[holder.size()];
      
      System.out.println("total bytes: " + holder.size());
      for (int i = 0; i < result.length; i++)
      {
         result[i] = holder.get(i).byteValue();
      }
      
      String txt = new String(result);
      System.out.println("the result: " + txt);
      
   }

}
