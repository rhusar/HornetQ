/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.buffers;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.SimpleString;

/**
 * A ResetLimitWrappedHornetQBuffer
 *
 * @author Tim Fox
 *
 */
public class ResetLimitWrappedHornetQBuffer implements HornetQBuffer
{      
   private static final Logger log = Logger.getLogger(ResetLimitWrappedHornetQBuffer.class);

   private final int limit;
   
   private HornetQBuffer buffer;
      
   public ResetLimitWrappedHornetQBuffer(final int limit, final HornetQBuffer buffer)
   {
      this.limit = limit;
      
      this.buffer = buffer;
      
      this.resetReaderIndex();
   }
   
   public void setBuffer(HornetQBuffer buffer)
   {      
      this.buffer = buffer;
   }
   
   public byte[] array()
   {
      return buffer.array();
   }

   public int capacity()
   {
      return buffer.capacity();
   }

   public void clear()
   {
      buffer.clear();
      
      buffer.setIndex(limit, limit);
   }

   public HornetQBuffer copy()
   {
      return buffer.copy();
   }

   public Object getUnderlyingBuffer()
   {
      return buffer.getUnderlyingBuffer();
   }

   public boolean readable()
   {
      return buffer.readable();
   }

   public int readableBytes()
   {
      return buffer.readableBytes();
   }

   public boolean readBoolean()
   {
      return buffer.readBoolean();
   }

   public byte readByte()
   {
      return buffer.readByte();
   }

   public void readBytes(byte[] bytes, int offset, int length)
   {
      buffer.readBytes(bytes, offset, length);
   }

   public void readBytes(byte[] bytes)
   {
      buffer.readBytes(bytes);
   }

   public char readChar()
   {
      return buffer.readChar();
   }

   public double readDouble()
   {
      return buffer.readDouble();
   }

   public int readerIndex()
   {
      return buffer.readerIndex();
   }

   public void readerIndex(int readerIndex)
   {
      if (readerIndex < limit)
      {
         readerIndex = limit;
      }
      
      buffer.readerIndex(readerIndex);
   }

   public float readFloat()
   {
      return buffer.readFloat();
   }

   public int readInt()
   {
      return buffer.readInt();
   }

   public int readInt(int pos)
   {
      return buffer.readInt(pos);
   }

   public long readLong()
   {
      return buffer.readLong();
   }

   public SimpleString readNullableSimpleString()
   {
      return buffer.readNullableSimpleString();
   }

   public String readNullableString()
   {
      return buffer.readNullableString();
   }

   public short readShort()
   {
      return buffer.readShort();
   }

   public SimpleString readSimpleString()
   {
      return buffer.readSimpleString();
   }

   public String readString()
   {
      return buffer.readString();
   }

   public short readUnsignedByte()
   {
      return buffer.readUnsignedByte();
   }

   public int readUnsignedShort()
   {
      return buffer.readUnsignedShort();
   }

   public String readUTF() throws Exception
   {
      return buffer.readUTF();
   }

   public void resetReaderIndex()
   {
      buffer.readerIndex(limit);
   }

   public void resetWriterIndex()
   {
      buffer.writerIndex(limit);
   }

   public void setIndex(int readerIndex, int writerIndex)
   {
      if (readerIndex < limit)
      {
         readerIndex = limit;
      }
      if (writerIndex < limit)
      {
         writerIndex = limit;
      }
      buffer.setIndex(readerIndex, writerIndex);
   }

   public void setInt(int pos, int val)
   {
      buffer.setInt(pos, val);
   }

   public HornetQBuffer slice(int index, int length)
   {
      return buffer.slice(index, length);
   }

   public boolean writable()
   {
      return buffer.writable();
   }

   public int writableBytes()
   {
      return buffer.writableBytes();
   }

   public void writeBoolean(boolean val)
   {
      buffer.writeBoolean(val);
   }

   public void writeByte(byte val)
   {
      buffer.writeByte(val);
   }

   public void writeBytes(byte[] bytes, int offset, int length)
   {
      buffer.writeBytes(bytes, offset, length);
   }

   public void writeBytes(byte[] bytes)
   {
      buffer.writeBytes(bytes);
   }

   public void writeBytes(HornetQBuffer src, int srcIndex, int length)
   {
      buffer.writeBytes(src, srcIndex, length);
   }

   public void writeChar(char val)
   {
      buffer.writeChar(val);
   }

   public void writeDouble(double val)
   {
      buffer.writeDouble(val);
   }

   public void writeFloat(float val)
   {
      buffer.writeFloat(val);
   }

   public void writeInt(int val)
   {
      buffer.writeInt(val);
   }

   public void writeLong(long val)
   {
      buffer.writeLong(val);
   }

   public void writeNullableSimpleString(SimpleString val)
   {
      buffer.writeNullableSimpleString(val);
   }

   public void writeNullableString(String val)
   {
      buffer.writeNullableString(val);
   }

   public int writerIndex()
   {
      return buffer.writerIndex();
   }

   public void writerIndex(int writerIndex)
   {
      if (writerIndex < limit)
      {
         writerIndex = limit;
      }
      buffer.writerIndex(writerIndex);
   }

   public void writeShort(short val)
   {
      buffer.writeShort(val);
   }

   public void writeSimpleString(SimpleString val)
   {
      buffer.writeSimpleString(val);
   }

   public void writeString(String val)
   {
      buffer.writeString(val);
   }

   public void writeUTF(String utf) throws Exception
   {
      buffer.writeUTF(utf);
   }

}
