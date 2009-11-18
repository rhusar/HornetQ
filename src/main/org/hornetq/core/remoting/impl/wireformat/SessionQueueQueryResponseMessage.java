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

package org.hornetq.core.remoting.impl.wireformat;

import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A SessionQueueQueryResponseMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionQueueQueryResponseMessage extends PacketImpl
{
   private boolean exists;

   private boolean durable;

   private int consumerCount;

   private int messageCount;

   private SimpleString filterString;

   private SimpleString address;

   public SessionQueueQueryResponseMessage(final boolean durable,
                                           final int consumerCount,
                                           final int messageCount,
                                           final SimpleString filterString,
                                           final SimpleString address)
   {
      this(durable, consumerCount, messageCount, filterString, address, true);
   }

   public SessionQueueQueryResponseMessage()
   {
      this(false, 0, 0, null, null, false);
   }

   private SessionQueueQueryResponseMessage(final boolean durable,
                                            final int consumerCount,
                                            final int messageCount,
                                            final SimpleString filterString,
                                            final SimpleString address,
                                            final boolean exists)
   {
      super(SESS_QUEUEQUERY_RESP);

      this.durable = durable;

      this.consumerCount = consumerCount;

      this.messageCount = messageCount;

      this.filterString = filterString;

      this.address = address;

      this.exists = exists;
   }

   public boolean isResponse()
   {
      return true;
   }

   public boolean isExists()
   {
      return exists;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public int getConsumerCount()
   {
      return consumerCount;
   }

   public int getMessageCount()
   {
      return messageCount;
   }

   public SimpleString getFilterString()
   {
      return filterString;
   }

   public SimpleString getAddress()
   {
      return address;
   }
   

   public int getRequiredBufferSize()
   {
      return PACKET_HEADERS_SIZE + 
      DataConstants.SIZE_BOOLEAN + // buffer.writeBoolean(exists);
      DataConstants.SIZE_BOOLEAN + // buffer.writeBoolean(durable);
      DataConstants.SIZE_INT + // buffer.writeInt(consumerCount);
      DataConstants.SIZE_INT + // buffer.writeInt(messageCount);
      SimpleString.sizeofNullableString(filterString) + // buffer.writeNullableSimpleString(filterString);
      SimpleString.sizeofNullableString(address); // buffer.writeNullableSimpleString(address);
   }

   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeBoolean(exists);
      buffer.writeBoolean(durable);
      buffer.writeInt(consumerCount);
      buffer.writeInt(messageCount);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeNullableSimpleString(address);
   }

   public void decodeRest(final HornetQBuffer buffer)
   {
      exists = buffer.readBoolean();
      durable = buffer.readBoolean();
      consumerCount = buffer.readInt();
      messageCount = buffer.readInt();
      filterString = buffer.readNullableSimpleString();
      address = buffer.readNullableSimpleString();
   }

   public boolean equals(Object other)
   {
      if (other instanceof SessionQueueQueryResponseMessage == false)
      {
         return false;
      }

      SessionQueueQueryResponseMessage r = (SessionQueueQueryResponseMessage)other;

      return super.equals(other) && this.exists == r.exists &&
             this.durable == r.durable &&
             this.consumerCount == r.consumerCount &&
             this.messageCount == r.messageCount &&
             this.filterString == null ? r.filterString == null
                                      : this.filterString.equals(r.filterString) && this.address == null ? r.address == null
                                                                                                        : this.address.equals(r.address);
   }

}
