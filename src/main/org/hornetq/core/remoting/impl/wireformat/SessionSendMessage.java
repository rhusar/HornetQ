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

import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.Message;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.integration.transports.netty.ChannelBufferWrapper;
import org.hornetq.utils.DataConstants;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert Suconic</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionSendMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SessionSendMessage.class);

   // Attributes ----------------------------------------------------

   private Message sentMessage;

   private ServerMessage receivedMessage;

   private boolean requiresResponse;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendMessage(final Message message, final boolean requiresResponse)
   {
      super(SESS_SEND);

      sentMessage = message;

      this.requiresResponse = requiresResponse;
   }

   public SessionSendMessage()
   {
      super(SESS_SEND);
   }

   // Public --------------------------------------------------------

   public Message getClientMessage()
   {
      return sentMessage;
   }

   public ServerMessage getServerMessage()
   {
      return receivedMessage;
   }

   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }

   @Override
   public HornetQBuffer encode(final RemotingConnection connection)
   {
      /*
       * We write the message to the buffer in the following structure:
       * 
       * First the standard packet headers - all packets have these
       * 
       * length:int
       * packet type:byte
       * channelID:long
       *
       * Then the message body:
       * 
       * bodySize:int
       * body:byte[]
       * 
       * {Note we store the message body before the message headers/properties since this allows the user to 
       * construct a message, add stuff to the body buffer, and send it without us having to copy the body into a new
       * buffer before sending it, this minmises buffer copying}
       * 
       * Then followed by the message headers and properties:
       * 
       * messageID:long
       * destination:SimpleString
       * message type: byte
       * durable: boolean
       * expiration: long
       * timestamp: long
       * priority: byte
       * 
       * properties: byte[]
       * 
       *  
       */
      HornetQBuffer buffer = sentMessage.getBuffer();

      // The body will already be written (if any) at this point, so we take note of the position of the end of the
      // body
      int afterBody = buffer.writerIndex();

      // We now write the message headers and properties
      sentMessage.encodeHeadersAndProperties(buffer);

      // We now write the extra data for the packet
      buffer.writeBoolean(requiresResponse);

      // We take note of the overall size of the packet
      size = buffer.writerIndex();
      
      // We now set the standard packet headers at the beginning of the buffer

      buffer.writerIndex(0);
      
      int len = size - DataConstants.SIZE_INT;
      buffer.writeInt(len);
      buffer.writeByte(type);
      buffer.writeLong(channelID);

      // This last byte we write marks the position of the end of the message body
      buffer.writeInt(afterBody);

      // And we set the indexes back for reading and writing
      buffer.setIndex(0, size);
            
      //We must make a copy of the buffer, since the message might get sent again, and the body might get read or written
      //this might occur while the same send is in operatio since netty send is asynch
      //this could cause incorrect data to be send and/or reader/writer positions to become corrupted
      
      HornetQBuffer newBuffer = buffer.copy();
      
      newBuffer.setIndex(0, afterBody);
      
      this.sentMessage.setBuffer(newBuffer);

      return buffer;
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      receivedMessage = new ServerMessageImpl();

      sentMessage = receivedMessage;

      // At this point, the standard packet headers will already have been read

      // We read the position of the end of the body - this is where the message headers and properties are stored
      int afterBody = buffer.readInt();

      // We now read message headers/properties

      buffer.setIndex(afterBody, buffer.writerIndex());

      receivedMessage.decode(buffer);

      // And we read extra data in the packet

      requiresResponse = buffer.readBoolean();

      // We set reader index back to the beginning of the buffer so it can be easily read if then delivered
      // to a client, and we set writer index to just after where the headers/properties were encoded so that it can
      // be fileld in with extra data required when delivering the packet to the client (e.g. delivery count, consumer
      // id)

      buffer.setIndex(0, buffer.writerIndex() - DataConstants.SIZE_BOOLEAN);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
