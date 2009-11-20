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

package org.hornetq.core.message.impl;

import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.PACKET_HEADERS_SIZE;
import static org.hornetq.utils.DataConstants.SIZE_BOOLEAN;
import static org.hornetq.utils.DataConstants.SIZE_BYTE;
import static org.hornetq.utils.DataConstants.SIZE_LONG;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.hornetq.core.buffers.HornetQChannelBuffers;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.message.Message;
import org.hornetq.core.message.PropertyConversionException;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;

/**
 * A concrete implementation of a message
 *
 * All messages handled by HornetQ core are of this type
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision: 2740 $</tt>
 *
 *
 * $Id: MessageSupport.java 2740 2007-05-30 11:36:28Z timfox $
 */
public abstract class MessageImpl implements Message
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MessageImpl.class);

   public static final SimpleString HDR_ACTUAL_EXPIRY_TIME = new SimpleString("_HQ_ACTUAL_EXPIRY");

   public static final SimpleString HDR_ORIGINAL_DESTINATION = new SimpleString("_HQ_ORIG_DESTINATION");

   public static final SimpleString HDR_ORIG_MESSAGE_ID = new SimpleString("_HQ_ORIG_MESSAGE_ID");

   public static final SimpleString HDR_GROUP_ID = new SimpleString("_HQ_GROUP_ID");

   public static final SimpleString HDR_SCHEDULED_DELIVERY_TIME = new SimpleString("_HQ_SCHED_DELIVERY");

   public static final SimpleString HDR_DUPLICATE_DETECTION_ID = new SimpleString("_HQ_DUPL_ID");

   public static final SimpleString HDR_ROUTE_TO_IDS = new SimpleString("_HQ_ROUTE_TO");

   public static final SimpleString HDR_FROM_CLUSTER = new SimpleString("_HQ_FROM_CLUSTER");

   public static final SimpleString HDR_LAST_VALUE_NAME = new SimpleString("_HQ_LVQ_NAME");

   // Attributes ----------------------------------------------------

   protected long messageID;

   protected SimpleString destination;

   protected byte type;

   protected boolean durable;

   /** GMT milliseconds at which this message expires. 0 means never expires * */
   protected long expiration;

   protected long timestamp;

   protected TypedProperties properties;

   protected byte priority;

   protected HornetQBuffer buffer;
   
   private int encodeSize;
   
   //This means does the buffer contain an accurate encoding of the message?
   protected boolean encodedToBuffer;
      
   // Constructors --------------------------------------------------

   protected MessageImpl()
   {
      properties = new TypedProperties();
   }

   /**
    * overridden by the client message, we need access to the connection so we can create the appropriate HornetQBuffer.
    * @param type
    * @param durable
    * @param expiration
    * @param timestamp
    * @param priority
    * @param body
    */
   protected MessageImpl(final byte type,
                         final boolean durable,
                         final long expiration,
                         final long timestamp,
                         final byte priority,
                         final HornetQBuffer buffer)
   {
      this();
      this.type = type;
      this.durable = durable;
      this.expiration = expiration;
      this.timestamp = timestamp;
      this.priority = priority;
      this.buffer = buffer;
   }
   
   protected MessageImpl(final long messageID)
   {
      this();
      this.messageID = messageID;
   }

   // Message implementation ----------------------------------------

   public void afterSend()
   {      
   }
   
   public boolean isBufferWritten()
   {
      return false;
   }
   
   public void resetBuffer()
   {     
      //There is a bug in Netty which requires us to initially write a byte
      if (buffer.capacity() == 0)
      {
         buffer.writeByte((byte)0);
      }

      buffer.setIndex(0, PacketImpl.PACKET_HEADERS_SIZE + DataConstants.SIZE_INT);        
   }
   
   public int getEncodeSize()
   {
      return encodeSize;
   }
  
   public int getBodySize()
   {
      return buffer.writerIndex();
   }

   public void encodeHeadersAndProperties(final HornetQBuffer buffer)
   {     
      buffer.writeLong(messageID);          
      buffer.writeSimpleString(destination);    
      buffer.writeByte(type);
      buffer.writeBoolean(durable);
      buffer.writeLong(expiration);
      buffer.writeLong(timestamp);
      buffer.writeByte(priority);
      properties.encode(buffer);
      encodeSize = buffer.writerIndex();
   }
   
   public void decodeFromWire(final HornetQBuffer buffer)
   {
      decodeHeadersAndProperties(buffer);

      this.buffer = buffer;
      
      this.encodedToBuffer = true;
   }
   
   public boolean isEncodedToBuffer()
   {
      return this.encodedToBuffer;
   }
       
   public void decodeHeadersAndProperties(final HornetQBuffer buffer)
   {     
      messageID = buffer.readLong();    
      destination = buffer.readSimpleString();         
      type = buffer.readByte();
      durable = buffer.readBoolean();
      expiration = buffer.readLong();
      timestamp = buffer.readLong();
      priority = buffer.readByte();
      properties.decode(buffer);
      encodeSize = buffer.readerIndex();
   }

   public long getMessageID()
   {
      return messageID;
   }

   public SimpleString getDestination()
   {
      return destination;
   }

   public void setDestination(final SimpleString destination)
   {
      this.destination = destination;
   }

   public byte getType()
   {
      return type;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public void setDurable(final boolean durable)
   {
      this.durable = durable;
   }

   public long getExpiration()
   {
      return expiration;
   }

   public void setExpiration(final long expiration)
   {
      this.expiration = expiration;
   }

   public long getTimestamp()
   {
      return timestamp;
   }

   public void setTimestamp(final long timestamp)
   {
      this.timestamp = timestamp;
   }

   public byte getPriority()
   {
      return priority;
   }

   public void setPriority(final byte priority)
   {
      this.priority = priority;
   }

   public boolean isExpired()
   {
      if (expiration == 0)
      {
         return false;
      }

      return System.currentTimeMillis() - expiration >= 0;
   }

 

   public Map<String, Object> toMap()
   {
      Map<String, Object> map = new HashMap<String, Object>();

      map.put("messageID", messageID);
      map.put("destination", destination.toString());
      map.put("type", type);
      map.put("durable", durable);
      map.put("expiration", expiration);
      map.put("timestamp", timestamp);
      map.put("priority", priority);
      for (SimpleString propName : properties.getPropertyNames())
      {
         map.put(propName.toString(), properties.getProperty(propName));
      }
      return map;
   }

   // Properties
   // ---------------------------------------------------------------------------------------

   public void putBooleanProperty(final SimpleString key, final boolean value)
   {
      properties.putBooleanProperty(key, value);
   }

   public void putByteProperty(final SimpleString key, final byte value)
   {
      properties.putByteProperty(key, value);
   }

   public void putBytesProperty(final SimpleString key, final byte[] value)
   {
      properties.putBytesProperty(key, value);
   }

   public void putShortProperty(final SimpleString key, final short value)
   {
      properties.putShortProperty(key, value);
   }

   public void putIntProperty(final SimpleString key, final int value)
   {
      properties.putIntProperty(key, value);
   }

   public void putLongProperty(final SimpleString key, final long value)
   {
      properties.putLongProperty(key, value);
   }

   public void putFloatProperty(final SimpleString key, final float value)
   {
      properties.putFloatProperty(key, value);
   }

   public void putDoubleProperty(final SimpleString key, final double value)
   {
      properties.putDoubleProperty(key, value);
   }

   public void putStringProperty(final SimpleString key, final SimpleString value)
   {
      properties.putSimpleStringProperty(key, value);
   }

   public void putObjectProperty(final SimpleString key, final Object value) throws PropertyConversionException
   {
      if (value == null)
      {
         // This is ok - when we try to read the same key it will return null too
         return;
      }

      if (value instanceof Boolean)
      {
         properties.putBooleanProperty(key, (Boolean)value);
      }
      else if (value instanceof Byte)
      {
         properties.putByteProperty(key, (Byte)value);
      }
      else if (value instanceof Short)
      {
         properties.putShortProperty(key, (Short)value);
      }
      else if (value instanceof Integer)
      {
         properties.putIntProperty(key, (Integer)value);
      }
      else if (value instanceof Long)
      {
         properties.putLongProperty(key, (Long)value);
      }
      else if (value instanceof Float)
      {
         properties.putFloatProperty(key, (Float)value);
      }
      else if (value instanceof Double)
      {
         properties.putDoubleProperty(key, (Double)value);
      }
      else if (value instanceof String)
      {
         properties.putSimpleStringProperty(key, new SimpleString((String)value));
      }
      else
      {
         throw new PropertyConversionException(value.getClass() + " is not a valid property type");
      }
   }

   public void putObjectProperty(final String key, final Object value) throws PropertyConversionException
   {
      putObjectProperty(new SimpleString(key), value);
   }

   public void putBooleanProperty(final String key, final boolean value)
   {
      properties.putBooleanProperty(new SimpleString(key), value);
   }

   public void putByteProperty(final String key, final byte value)
   {
      properties.putByteProperty(new SimpleString(key), value);
   }

   public void putBytesProperty(final String key, final byte[] value)
   {
      properties.putBytesProperty(new SimpleString(key), value);
   }

   public void putShortProperty(final String key, final short value)
   {
      properties.putShortProperty(new SimpleString(key), value);
   }

   public void putIntProperty(final String key, final int value)
   {
      properties.putIntProperty(new SimpleString(key), value);
   }

   public void putLongProperty(final String key, final long value)
   {
      properties.putLongProperty(new SimpleString(key), value);
   }

   public void putFloatProperty(final String key, final float value)
   {
      properties.putFloatProperty(new SimpleString(key), value);
   }

   public void putDoubleProperty(final String key, final double value)
   {
      properties.putDoubleProperty(new SimpleString(key), value);
   }

   public void putStringProperty(final String key, final String value)
   {
      properties.putSimpleStringProperty(new SimpleString(key), new SimpleString(value));
   }

   public void putTypedProperties(final TypedProperties otherProps)
   {
      properties.putTypedProperties(otherProps);
   }

   public Object getObjectProperty(final SimpleString key)
   {
      return properties.getProperty(key);
   }

   public Boolean getBooleanProperty(final SimpleString key) throws PropertyConversionException
   {
      return properties.getBooleanProperty(key);
   }

   public Boolean getBooleanProperty(final String key) throws PropertyConversionException
   {
      return properties.getBooleanProperty(new SimpleString(key));
   }

   public Byte getByteProperty(final SimpleString key) throws PropertyConversionException
   {
      return properties.getByteProperty(key);
   }

   public Byte getByteProperty(final String key) throws PropertyConversionException
   {
      return properties.getByteProperty(new SimpleString(key));
   }

   public byte[] getBytesProperty(final SimpleString key) throws PropertyConversionException
   {
      return properties.getBytesProperty(key);
   }

   public byte[] getBytesProperty(final String key) throws PropertyConversionException
   {
      return getBytesProperty(new SimpleString(key));
   }

   public Double getDoubleProperty(final SimpleString key) throws PropertyConversionException
   {
      return properties.getDoubleProperty(key);
   }

   public Double getDoubleProperty(final String key) throws PropertyConversionException
   {
      return properties.getDoubleProperty(new SimpleString(key));
   }

   public Integer getIntProperty(final SimpleString key) throws PropertyConversionException
   {
      return properties.getIntProperty(key);
   }

   public Integer getIntProperty(final String key) throws PropertyConversionException
   {
      return properties.getIntProperty(new SimpleString(key));
   }

   public Long getLongProperty(final SimpleString key) throws PropertyConversionException
   {
      return properties.getLongProperty(key);
   }

   public Long getLongProperty(final String key) throws PropertyConversionException
   {
      return properties.getLongProperty(new SimpleString(key));
   }

   public Short getShortProperty(final SimpleString key) throws PropertyConversionException
   {
      return properties.getShortProperty(key);
   }

   public Short getShortProperty(final String key) throws PropertyConversionException
   {
      return properties.getShortProperty(new SimpleString(key));
   }

   public Float getFloatProperty(final SimpleString key) throws PropertyConversionException
   {
      return properties.getFloatProperty(key);
   }

   public Float getFloatProperty(final String key) throws PropertyConversionException
   {
      return properties.getFloatProperty(new SimpleString(key));
   }

   public String getStringProperty(final SimpleString key) throws PropertyConversionException
   {
      SimpleString str = getSimpleStringProperty(key);

      if (str == null)
      {
         return null;
      }
      else
      {
         return str.toString();
      }
   }

   public String getStringProperty(final String key) throws PropertyConversionException
   {
      return getStringProperty(new SimpleString(key));
   }

   public SimpleString getSimpleStringProperty(final SimpleString key) throws PropertyConversionException
   {
      return properties.getSimpleStringProperty(key);
   }

   public SimpleString getSimpleStringProperty(final String key) throws PropertyConversionException
   {
      return properties.getSimpleStringProperty(new SimpleString(key));
   }

   public Object getObjectProperty(final String key)
   {
      return properties.getProperty(new SimpleString(key));
   }

   public Object removeProperty(final SimpleString key)
   {
      return properties.removeProperty(key);
   }

   public Object removeProperty(final String key)
   {
      return properties.removeProperty(new SimpleString(key));
   }

   public boolean containsProperty(final SimpleString key)
   {
      return properties.containsProperty(key);
   }

   public boolean containsProperty(final String key)
   {
      return properties.containsProperty(new SimpleString(key));
   }

   public Set<SimpleString> getPropertyNames()
   {
      return properties.getPropertyNames();
   }

   public TypedProperties getProperties()
   {
      return properties;
   }

   public HornetQBuffer getBuffer()
   {
      return buffer;
   }
   
   public void setBuffer(HornetQBuffer buffer)
   {
      this.buffer = buffer;
   }

   public BodyEncoder getBodyEncoder()
   {
      return new DecodingContext();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
   private class DecodingContext implements BodyEncoder
   {
      private int lastPos = 0;

      public DecodingContext()
      {
      }

      public void open()
      {
      }

      public void close()
      {
      }

      public int encode(ByteBuffer bufferRead) throws HornetQException
      {
         HornetQBuffer buffer = HornetQChannelBuffers.wrappedBuffer(bufferRead);
         return encode(buffer, bufferRead.capacity());
      }

      public int encode(HornetQBuffer bufferOut, int size)
      {
         bufferOut.writeBytes(getBuffer(), lastPos, size);
         lastPos += size;
         return size;
      }
   }
   
   //FIXME - all this stuff only used by large messages, move it!
   
   public int getHeadersAndPropertiesEncodeSize()
   {
      return SIZE_LONG + /* Destination */SimpleString.sizeofString(destination) +
      /* Type */SIZE_BYTE +
      /* Durable */SIZE_BOOLEAN +
      /* Expiration */SIZE_LONG +
      /* Timestamp */SIZE_LONG +
      /* Priority */SIZE_BYTE +
      /* PropertySize and Properties */properties.getEncodeSize();
   }

}
