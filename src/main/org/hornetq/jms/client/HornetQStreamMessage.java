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

package org.hornetq.jms.client;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.DataConstants;

/**
 * This class implements javax.jms.StreamMessage.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Some parts based on JBM 1.x class by:
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * 
 * @version $Revision: 3412 $
 *
 * $Id: HornetQRAStreamMessage.java 3412 2007-12-05 19:41:47Z timfox $
 */
public class HornetQStreamMessage extends HornetQMessage implements StreamMessage
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(HornetQStreamMessage.class);

   public static final byte TYPE = 6;

   // Attributes ----------------------------------------------------

   private HornetQBuffer buffer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public HornetQStreamMessage(final ClientSession session)
   {
      super(HornetQStreamMessage.TYPE, session);
   }

   public HornetQStreamMessage(final ClientMessage message, final ClientSession session)
   {
      super(message, session);
   }

   public HornetQStreamMessage(final StreamMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, HornetQStreamMessage.TYPE, session);

      foreign.reset();

      try
      {
         while (true)
         {
            Object obj = foreign.readObject();
            writeObject(obj);
         }
      }
      catch (MessageEOFException e)
      {
         // Ignore
      }
   }

   // Public --------------------------------------------------------

   @Override
   public byte getType()
   {
      return HornetQStreamMessage.TYPE;
   }

   // StreamMessage implementation ----------------------------------

   public boolean readBoolean() throws JMSException
   {
      checkRead();
      try
      {
         byte type = buffer.readByte();

         switch (type)
         {
            case DataConstants.BOOLEAN:
               return buffer.readBoolean();
            case DataConstants.STRING:
               String s = buffer.readNullableString();
               return Boolean.valueOf(s);
            default:
               throw new MessageFormatException("Invalid conversion");
         }
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public byte readByte() throws JMSException
   {
      checkRead();
      try
      {
         byte type = buffer.readByte();
         switch (type)
         {
            case DataConstants.BYTE:
               return buffer.readByte();
            case DataConstants.STRING:
               String s = buffer.readNullableString();
               return Byte.parseByte(s);
            default:
               throw new MessageFormatException("Invalid conversion");
         }
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public short readShort() throws JMSException
   {
      checkRead();
      try
      {
         byte type = buffer.readByte();
         switch (type)
         {
            case DataConstants.BYTE:
               return buffer.readByte();
            case DataConstants.SHORT:
               return buffer.readShort();
            case DataConstants.STRING:
               String s = buffer.readNullableString();
               return Short.parseShort(s);
            default:
               throw new MessageFormatException("Invalid conversion");
         }
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public char readChar() throws JMSException
   {
      checkRead();
      try
      {
         byte type = buffer.readByte();
         switch (type)
         {
            case DataConstants.CHAR:
               return buffer.readChar();
            default:
               throw new MessageFormatException("Invalid conversion");
         }
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readInt() throws JMSException
   {
      checkRead();
      try
      {
         byte type = buffer.readByte();
         switch (type)
         {
            case DataConstants.BYTE:
               return buffer.readByte();
            case DataConstants.SHORT:
               return buffer.readShort();
            case DataConstants.INT:
               return buffer.readInt();
            case DataConstants.STRING:
               String s = buffer.readNullableString();
               return Integer.parseInt(s);
            default:
               throw new MessageFormatException("Invalid conversion");
         }
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public long readLong() throws JMSException
   {
      checkRead();
      try
      {
         byte type = buffer.readByte();
         switch (type)
         {
            case DataConstants.BYTE:
               return buffer.readByte();
            case DataConstants.SHORT:
               return buffer.readShort();
            case DataConstants.INT:
               return buffer.readInt();
            case DataConstants.LONG:
               return buffer.readLong();
            case DataConstants.STRING:
               String s = buffer.readNullableString();
               return Long.parseLong(s);
            default:
               throw new MessageFormatException("Invalid conversion");
         }
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public float readFloat() throws JMSException
   {
      checkRead();
      try
      {
         byte type = buffer.readByte();
         switch (type)
         {
            case DataConstants.FLOAT:
               return buffer.readFloat();
            case DataConstants.STRING:
               String s = buffer.readNullableString();
               return Float.parseFloat(s);
            default:
               throw new MessageFormatException("Invalid conversion");
         }
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public double readDouble() throws JMSException
   {
      checkRead();
      try
      {
         byte type = buffer.readByte();
         switch (type)
         {
            case DataConstants.FLOAT:
               return buffer.readFloat();
            case DataConstants.DOUBLE:
               return buffer.readDouble();
            case DataConstants.STRING:
               String s = buffer.readNullableString();
               return Double.parseDouble(s);
            default:
               throw new MessageFormatException("Invalid conversion: " + type);
         }
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public String readString() throws JMSException
   {
      checkRead();
      try
      {
         byte type = buffer.readByte();
         switch (type)
         {
            case DataConstants.BOOLEAN:
               return String.valueOf(buffer.readBoolean());
            case DataConstants.BYTE:
               return String.valueOf(buffer.readByte());
            case DataConstants.SHORT:
               return String.valueOf(buffer.readShort());
            case DataConstants.CHAR:
               return String.valueOf(buffer.readChar());
            case DataConstants.INT:
               return String.valueOf(buffer.readInt());
            case DataConstants.LONG:
               return String.valueOf(buffer.readLong());
            case DataConstants.FLOAT:
               return String.valueOf(buffer.readFloat());
            case DataConstants.DOUBLE:
               return String.valueOf(buffer.readDouble());
            case DataConstants.STRING:
               return buffer.readNullableString();
            default:
               throw new MessageFormatException("Invalid conversion");
         }
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   private int len;

   public int readBytes(final byte[] value) throws JMSException
   {
      checkRead();
      try
      {
         if (len == -1)
         {
            len = 0;
            return -1;
         }
         else if (len == 0)
         {
            byte type = buffer.readByte();
            if (type != DataConstants.BYTES)
            {
               throw new MessageFormatException("Invalid conversion");
            }
            len = buffer.readInt();
         }
         int read = Math.min(value.length, len);
         buffer.readBytes(value, 0, read);
         len -= read;
         if (len == 0)
         {
            len = -1;
         }
         return read;
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public Object readObject() throws JMSException
   {
      checkRead();
      byte type = buffer.readByte();
      switch (type)
      {
         case DataConstants.BOOLEAN:
            return buffer.readBoolean();
         case DataConstants.BYTE:
            return buffer.readByte();
         case DataConstants.SHORT:
            return buffer.readShort();
         case DataConstants.CHAR:
            return buffer.readChar();
         case DataConstants.INT:
            return buffer.readInt();
         case DataConstants.LONG:
            return buffer.readLong();
         case DataConstants.FLOAT:
            return buffer.readFloat();
         case DataConstants.DOUBLE:
            return buffer.readDouble();
         case DataConstants.STRING:
            return buffer.readNullableString();
         case DataConstants.BYTES:
            int len = buffer.readInt();
            byte[] bytes = new byte[len];
            buffer.readBytes(bytes);
            return bytes;
         default:
            throw new MessageFormatException("Invalid conversion");
      }
   }

   public void writeBoolean(final boolean value) throws JMSException
   {
      checkWrite();
      buffer.writeByte(DataConstants.BOOLEAN);
      buffer.writeBoolean(value);
   }

   public void writeByte(final byte value) throws JMSException
   {
      checkWrite();
      buffer.writeByte(DataConstants.BYTE);
      buffer.writeByte(value);
   }

   public void writeShort(final short value) throws JMSException
   {
      checkWrite();
      buffer.writeByte(DataConstants.SHORT);
      buffer.writeShort(value);
   }

   public void writeChar(final char value) throws JMSException
   {
      checkWrite();
      buffer.writeByte(DataConstants.CHAR);
      buffer.writeChar(value);
   }

   public void writeInt(final int value) throws JMSException
   {
      checkWrite();
      buffer.writeByte(DataConstants.INT);
      buffer.writeInt(value);
   }

   public void writeLong(final long value) throws JMSException
   {
      checkWrite();
      buffer.writeByte(DataConstants.LONG);
      buffer.writeLong(value);
   }

   public void writeFloat(final float value) throws JMSException
   {
      checkWrite();
      buffer.writeByte(DataConstants.FLOAT);
      buffer.writeFloat(value);
   }

   public void writeDouble(final double value) throws JMSException
   {
      checkWrite();
      buffer.writeByte(DataConstants.DOUBLE);
      buffer.writeDouble(value);
   }

   public void writeString(final String value) throws JMSException
   {
      checkWrite();
      buffer.writeByte(DataConstants.STRING);
      buffer.writeNullableString(value);
   }

   public void writeBytes(final byte[] value) throws JMSException
   {
      checkWrite();
      buffer.writeByte(DataConstants.BYTES);
      buffer.writeInt(value.length);
      buffer.writeBytes(value);
   }

   public void writeBytes(final byte[] value, final int offset, final int length) throws JMSException
   {
      checkWrite();
      buffer.writeByte(DataConstants.BYTES);
      buffer.writeInt(length);
      buffer.writeBytes(value, offset, length);
   }

   public void writeObject(final Object value) throws JMSException
   {
      if (value == null)
      {
         throw new NullPointerException("Attempt to write a null value");
      }
      if (value instanceof String)
      {
         writeString((String)value);
      }
      else if (value instanceof Boolean)
      {
         writeBoolean((Boolean)value);
      }
      else if (value instanceof Byte)
      {
         writeByte((Byte)value);
      }
      else if (value instanceof Short)
      {
         writeShort((Short)value);
      }
      else if (value instanceof Integer)
      {
         writeInt((Integer)value);
      }
      else if (value instanceof Long)
      {
         writeLong((Long)value);
      }
      else if (value instanceof Float)
      {
         writeFloat((Float)value);
      }
      else if (value instanceof Double)
      {
         writeDouble((Double)value);
      }
      else if (value instanceof byte[])
      {
         writeBytes((byte[])value);
      }
      else if (value instanceof Character)
      {
         writeChar((Character)value);
      }
      else
      {
         throw new MessageFormatException("Invalid object type: " + value.getClass());
      }
   }

   public void reset() throws JMSException
   {
      if (!readOnly)
      {
         readOnly = true;
      }
      buffer.resetReaderIndex();
   }

   // HornetQRAMessage overrides ----------------------------------------

   @Override
   public void clearBody() throws JMSException
   {
      super.clearBody();

      buffer.clear();

   }

   @Override
   public void doBeforeSend() throws Exception
   {
      reset();

      message.encodeToBuffer();

      message.getBuffer().writeBytes(buffer, 0, buffer.writerIndex());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
