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
      
   @Override
   public HornetQBuffer encode(final RemotingConnection connection)
   {
      //We re-use the same packet buffer - but we need to change the extra data
      
      HornetQBuffer buffer = serverMessage.getBuffer();
           
      if (serverMessage.isEncodedToBuffer())
      {
         //It's already encoded - we just need to change the extra data at the end
         //so we need to jump to the after body position
         
         buffer.setIndex(0, serverMessage.getEndMessagePosition());         
      }
      else
      {
         int afterBody = buffer.writerIndex();
         
         //Message hasn't been encoded yet - probably it's something like a notification message generated on the server
         
         // We now write the message headers and properties
         serverMessage.encodeHeadersAndProperties(buffer);
         
         serverMessage.setEndMessagePosition(buffer.writerIndex());
         
         //Now we need to fill in the afterBody 
         buffer.setInt(PacketImpl.PACKET_HEADERS_SIZE, afterBody);
      }
                             
      buffer.writeLong(consumerID);
      buffer.writeInt(deliveryCount);
      
      // Calculate the new packet size
      size = buffer.writerIndex();
      
      buffer.setIndex(0, 0);

      // Fill in the standard header fields

      int len = size - DataConstants.SIZE_INT;
      buffer.writeInt(len);
      buffer.writeByte(type);
      buffer.writeLong(channelID);
                      
      //And fill in the message id, since this was set on the server side so won't already be in the buffer
      
      buffer.writerIndex(buffer.readInt(PacketImpl.PACKET_HEADERS_SIZE));            
      buffer.writeLong(serverMessage.getMessageID());
      
      //Set the reader and writer position to be read fully by remoting
      buffer.setIndex(0, size);

      return buffer;
   }

   @Override
   public void decode(final HornetQBuffer buffer)
   {
      channelID = buffer.readLong();
      
      clientMessage = new ClientMessageImpl();
      
      // We read the position of the end of the body - this is where the message headers and properties are stored
      int afterBody = buffer.readInt();
      
      //At this point standard headers have been decoded and we are positioned at the beginning of the body
      int bodyStart = buffer.readerIndex();
      
      // We now read message headers/properties

      buffer.readerIndex(afterBody);
            
      clientMessage.decodeFromWire(buffer);
      
      // And read the extra data
            
      consumerID = buffer.readLong();
        
      deliveryCount = buffer.readInt();
      
      clientMessage.setDeliveryCount(deliveryCount);
      
      size = buffer.readerIndex();

      // Set reader index back to beginning of body
      
      buffer.readerIndex(bodyStart);
      
      clientMessage.setBuffer(buffer);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
