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

package org.hornetq.integration.stomp;

import java.util.Map;

import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionCloseMessage;
import org.hornetq.utils.UUIDGenerator;
import org.hornetq.utils.VersionLoader;

/**
 * A ProtocolConverter
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class ProtocolConverter
{

   public Packet toPacket(StompFrame frame)
   {
      String command = frame.getCommand();
      Map<String, Object> headers = frame.getHeaders();
      if (Stomp.Commands.CONNECT.equals(command))
      {
         String login = (String)headers.get("login");
         String password = (String)headers.get("passcode");

         String name = UUIDGenerator.getInstance().generateStringUUID();
         long sessionChannelID = 12;
         return new CreateSessionMessage(name,
                                         sessionChannelID,
                                         VersionLoader.getVersion().getIncrementingVersion(),
                                         login,
                                         password,
                                         HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                         false,
                                         true,
                                         true,
                                         false,
                                         HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE);
      }
      if (Stomp.Commands.DISCONNECT.equals(command))
      {
         return new SessionCloseMessage();
      }
      else
      {
         throw new RuntimeException("frame not supported: " + frame);
      }
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
