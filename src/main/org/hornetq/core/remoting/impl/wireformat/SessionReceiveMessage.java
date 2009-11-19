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

import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.client.impl.ClientMessageInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionReceiveMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SessionReceiveMessage.class);

   // Attributes ----------------------------------------------------

   private long consumerID;

   private ClientMessageInternal clientMessage;

   private ServerMessage serverMessage;

   private int deliveryCount;

   public SessionReceiveMessage(final long consumerID, final ServerMessage message, final int deliveryCount)
   {
      super(SESS_RECEIVE_MSG);

      this.consumerID = consumerID;

      this.serverMessage = message;

      this.clientMessage = null;

      this.deliveryCount = deliveryCount;
   }

   public SessionReceiveMessage()
   {
      super(SESS_RECEIVE_MSG);
   }

   // Public --------------------------------------------------------

   public long getConsumerID()
   {
      return consumerID;
   }

   public ClientMessageInternal getClientMessage()
   {
      return clientMessage;
   }

   public ServerMessage getServerMessage()
   {
      return serverMessage;
   }

   public int getDeliveryCount()
   {
      return deliveryCount;
   }

   public int getRequiredBufferSize()
   {
      return PACKET_HEADERS_SIZE +
             // consumerID
             DataConstants.SIZE_LONG +
             // deliveryCount
             DataConstants.SIZE_INT +
             // isLargeMessage
             DataConstants.SIZE_BOOLEAN +
             // message.encoding
             (serverMessage != null ? serverMessage.getEncodeSize() : clientMessage.getEncodeSize());

   }
   
   @Override
   public HornetQBuffer encode(final RemotingConnection connection)
   {
      HornetQBuffer buffer = serverMessage.getBuffer();
      
      buffer.writeLong(consumerID);
      buffer.writeInt(deliveryCount);
      
      size = buffer.writerIndex();
      
      buffer.setIndex(0, 0);

      // The standard header fields

      int len = size - DataConstants.SIZE_INT;
      buffer.writeInt(len);
      buffer.writeByte(type);
      buffer.writeLong(channelID);
                      
      //And fill in the message id, since this was set on the server side so won't already be in the buffer
      buffer.setIndex(0, buffer.writerIndex() + DataConstants.SIZE_INT);
      buffer.writeLong(serverMessage.getMessageID());
      
      buffer.setIndex(0, size);

      return buffer;
   }

   public void decodeRest(final HornetQBuffer buffer)
   {
      clientMessage = new ClientMessageImpl();
      
      //fast forward past the size byte
      int size = buffer.readInt();
      
      clientMessage.decode(buffer);
      
      int bodyBeginning = buffer.readerIndex();
      
      clientMessage.setBuffer(buffer);
      
      //Now we need to fast forward past the body part
      
      //int size = buffer.readInt(PacketImpl.PACKET_HEADERS_SIZE);
      
      buffer.setIndex(size, buffer.writerIndex());
                  
      consumerID = buffer.readLong();
      
      deliveryCount = buffer.readInt();
      
      clientMessage.setDeliveryCount(deliveryCount);
      
      //Reset buffer to beginning of body
      buffer.setIndex(bodyBeginning, buffer.writerIndex());
      
      clientMessage.setBuffer(buffer);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
