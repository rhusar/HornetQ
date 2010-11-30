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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * A InflaterWriter
 * 
 * This class takes an OutputStream. Compressed bytes 
 * can directly be written into this class. The class will
 * decompress the bytes and write them to the output stream.
 * 
 * Not for concurrent use.
 * 
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public class InflaterWriter extends OutputStream
{
   private Inflater inflater = new Inflater();
   private OutputStream output;
   
   private byte[] writeBuffer = new byte[1024];
   private int writePointer = 0;
   
   private byte[] outputBuffer = new byte[writeBuffer.length*2];
   
   public InflaterWriter(OutputStream output)
   {
      this.output = output;
   }

   /*
    * Write a compressed byte.
    */
   @Override
   public void write(int b) throws IOException
   {
      writeBuffer[writePointer] = (byte)(b & 0xFF);
      writePointer++;
      
      if (writePointer == writeBuffer.length)
      {
         writePointer = 0;
         try
         {
            doWrite();
         }
         catch (DataFormatException e)
         {
            throw new IOException("Error decompressing data", e);
         }
      }
   }
   
   @Override
   public void close() throws IOException
   {
      if (writePointer > 0)
      {
         inflater.setInput(writeBuffer, 0, writePointer);
         try
         {
            int n = inflater.inflate(outputBuffer);
            while (n > 0)
            {
               output.write(outputBuffer, 0, n);
               n = inflater.inflate(outputBuffer);
            }
            output.close();
         }
         catch (DataFormatException e)
         {
            throw new IOException(e);
         }
      }
   }
   
   private void doWrite() throws DataFormatException, IOException
   {
      inflater.setInput(writeBuffer);
      int n = inflater.inflate(outputBuffer);
      
      while (n > 0)
      {
         output.write(outputBuffer, 0, n);
         n = inflater.inflate(outputBuffer);
      }
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
      
      ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
      InflaterWriter writer = new InflaterWriter(byteOutput);
      
      byte[] zipBuffer = new byte[12];
      
      int n = byteInput.read(zipBuffer);
      while (n > 0)
      {
         System.out.println("Writing: " + n);
         writer.write(zipBuffer, 0, n);
         n = byteInput.read(zipBuffer);
      }

      writer.close();
      
      byte[] outcome = byteOutput.toByteArray();
      String outStr = new String(outcome);
      
      System.out.println("Outcome: " + outStr);
      
   }

}
