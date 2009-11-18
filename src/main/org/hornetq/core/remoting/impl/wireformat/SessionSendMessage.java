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
import org.hornetq.utils.DataConstants;

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
      log.info("Encoding session send message");
      
      HornetQBuffer buffer = sentMessage.getBuffer();
      
      log.info("ENCODE ** size is " + buffer.writerIndex());
      
      buffer.writeBoolean(requiresResponse);

      // At this point, the rest of the message has already been encoded into the buffer
      size = buffer.writerIndex();
            

      buffer.setIndex(0, 0);

      // The standard header fields

      int len = size - DataConstants.SIZE_INT;
      buffer.writeInt(len);
      buffer.writeByte(type);
      buffer.writeLong(channelID);
      buffer.writeInt(size);
      
      buffer.setIndex(0, size);

      return buffer;
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      receivedMessage = new ServerMessageImpl();

      sentMessage = receivedMessage;
      
      //fast forward past the size byte
      buffer.readInt();

      log.info("********** server message ");
                 
      receivedMessage.decode(buffer);
            
      receivedMessage.getBuffer().resetReaderIndex();

      requiresResponse = buffer.readBoolean();
      
      //reset the writer index back one boolean since when we deliver to the client we will write the extra fields on here
      
      //buffer.setIndex(0, buffer.writerIndex() - DataConstants.SIZE_BOOLEAN);
      
      log.info("SEND MESSAGE DECODE, WRITER INDEX IS " + buffer.writerIndex());
   }

   public int getRequiredBufferSize()
   {
      int size = PACKET_HEADERS_SIZE + sentMessage.getEncodeSize() + DataConstants.SIZE_BOOLEAN;

      return size;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
