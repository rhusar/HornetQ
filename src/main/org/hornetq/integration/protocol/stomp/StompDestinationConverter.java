/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.integration.protocol.stomp;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.jms.client.HornetQQueue;
import org.hornetq.jms.client.HornetQTemporaryQueue;
import org.hornetq.jms.client.HornetQTemporaryTopic;
import org.hornetq.jms.client.HornetQTopic;

/**
 * A StompDestinationConverter
 *
 * @author jmesnil
 *
 *
 */
public class StompDestinationConverter
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static SimpleString convertDestination(String name) throws HornetQException
   {
      if (name == null)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "No destination is specified!");
      }
      else if (name.startsWith("/queue/"))
      {
         String queueName = name.substring("/queue/".length(), name.length());
         return HornetQQueue.createAddressFromName(queueName);
      }
      else if (name.startsWith("/topic/"))
      {
         String topicName = name.substring("/topic/".length(), name.length());
         return HornetQTopic.createAddressFromName(topicName);
      }
      else if (name.startsWith("/temp-queue/"))
      {
         String tempName = name.substring("/temp-queue/".length(), name.length());
         return HornetQTemporaryQueue.createAddressFromName(tempName);
      }
      else if (name.startsWith("/temp-topic/"))
      {
         String tempName = name.substring("/temp-topic/".length(), name.length());
         return HornetQTemporaryTopic.createAddressFromName(tempName);
      }
      else
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "Illegal destination name: [" + name +
                                                                    "] -- StompConnect destinations " +
                                                                    "must begine with one of: /queue/ /topic/ /temp-queue/ /temp-topic/");
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
