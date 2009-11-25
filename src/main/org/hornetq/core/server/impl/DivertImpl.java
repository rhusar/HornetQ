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

package org.hornetq.core.server.impl;

import static org.hornetq.core.message.impl.MessageImpl.HDR_ORIGINAL_DESTINATION;

import org.hornetq.core.filter.Filter;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.Divert;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.Transformer;
import org.hornetq.utils.SimpleString;

/**
 * A DivertImpl simply diverts a message to a different forwardAddress
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 19 Dec 2008 10:57:49
 *
 *
 */
public class DivertImpl implements Divert
{
   private static final Logger log = Logger.getLogger(DivertImpl.class);

   private final PostOffice postOffice;

   private final SimpleString forwardAddress;

   private final SimpleString uniqueName;

   private final SimpleString routingName;

   private final boolean exclusive;

   private final Filter filter;

   private final Transformer transformer;

   public DivertImpl(final SimpleString forwardAddress,
                     final SimpleString uniqueName,
                     final SimpleString routingName,
                     final boolean exclusive,
                     final Filter filter,
                     final Transformer transformer,
                     final PostOffice postOffice)
   {
      this.forwardAddress = forwardAddress;

      this.uniqueName = uniqueName;

      this.routingName = routingName;

      this.exclusive = exclusive;

      this.filter = filter;

      this.transformer = transformer;

      this.postOffice = postOffice;
   }

   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      SimpleString originalDestination = message.getDestination();

      // We must make a copy of the message, otherwise things like returning credits to the page won't work
      // properly on ack, since the original destination will be overwritten

      // TODO we can optimise this so it doesn't copy if it's not routed anywhere else

      log.info("making copy for divert");
      
      ServerMessage copy = message.copy();

      copy.setDestination(forwardAddress);

      copy.putStringProperty(HDR_ORIGINAL_DESTINATION, originalDestination);

      if (transformer != null)
      {
         copy = transformer.transform(copy);
      }

      postOffice.route(copy, context.getTransaction());
   }

   public SimpleString getRoutingName()
   {
      return routingName;
   }

   public SimpleString getUniqueName()
   {
      return uniqueName;
   }

   public boolean isExclusive()
   {
      return exclusive;
   }

   public Filter getFilter()
   {
      return filter;
   }
}
