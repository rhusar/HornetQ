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

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.Message;
import org.hornetq.core.server.impl.ServerMessageImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 */
public class SessionSendMessage extends MessagePacket
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SessionSendMessage.class);

   // Attributes ----------------------------------------------------

   private boolean requiresResponse;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendMessage(final Message message, final boolean requiresResponse)
   {
      super(SESS_SEND, message);
      
      this.requiresResponse = requiresResponse;
      
      message.forceCopy();
   }
   
   public SessionSendMessage()
   {
      super(SESS_SEND, new ServerMessageImpl());
   }

   // Public --------------------------------------------------------

   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void encodeExtraData(HornetQBuffer buffer)
   {
      buffer.writeBoolean(requiresResponse);
   }
   
   protected void decodeExtraData(HornetQBuffer buffer)
   {
      requiresResponse = buffer.readBoolean();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
