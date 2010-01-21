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
 * A StompUtils
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
class StompUtils
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static SimpleString toHornetQAddress(String stompDestination) throws HornetQException
   {
      if (stompDestination == null)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "No destination is specified!");
      }
      else if (stompDestination.startsWith("/queue/"))
      {
         String queueName = stompDestination.substring("/queue/".length(), stompDestination.length());
         return HornetQQueue.createAddressFromName(queueName);
      }
      else if (stompDestination.startsWith("/topic/"))
      {
         String topicName = stompDestination.substring("/topic/".length(), stompDestination.length());
         return HornetQTopic.createAddressFromName(topicName);
      }
      else if (stompDestination.startsWith("/temp-queue/"))
      {
         String tempName = stompDestination.substring("/temp-queue/".length(), stompDestination.length());
         return HornetQTemporaryQueue.createAddressFromName(tempName);
      }
      else if (stompDestination.startsWith("/temp-topic/"))
      {
         String tempName = stompDestination.substring("/temp-topic/".length(), stompDestination.length());
         return HornetQTemporaryTopic.createAddressFromName(tempName);
      }
      else
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "Illegal destination name: [" + stompDestination +
                                                                    "] -- StompConnect destinations " +
                                                                    "must begine with one of: /queue/ /topic/ /temp-queue/ /temp-topic/");
      }
   }

   public static String toStompDestination(String hornetqAddress) throws HornetQException
   {
      if (hornetqAddress == null)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "No destination is specified!");
      }
      else if (hornetqAddress.startsWith(HornetQQueue.JMS_QUEUE_ADDRESS_PREFIX))
      {
         return "/queue/" + hornetqAddress.substring(HornetQQueue.JMS_QUEUE_ADDRESS_PREFIX.length(),
                                                     hornetqAddress.length());
      }
      else if (hornetqAddress.startsWith(HornetQTemporaryQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX))
      {
         return "/temp-queue/" + hornetqAddress.substring(HornetQTemporaryQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX.length(),
                                                          hornetqAddress.length());
      }
      else if (hornetqAddress.startsWith(HornetQTopic.JMS_TOPIC_ADDRESS_PREFIX))
      {
         return "/topic/" + hornetqAddress.substring(HornetQTopic.JMS_TOPIC_ADDRESS_PREFIX.length(),
                                                     hornetqAddress.length());
      }
      else if (hornetqAddress.startsWith(HornetQTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX))
      {
         return "/temp-topic/" + hornetqAddress.substring(HornetQTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX.length(),
                                                          hornetqAddress.length());
      }
      else
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "Illegal address name: [" + hornetqAddress +
                                                                    "] -- Acceptable address must comply to JMS semantics");
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
