/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.core.server.group.impl;

import org.hornetq.core.management.NotificationType;
import org.hornetq.core.management.Notification;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.client.management.impl.ManagementHelper;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.ConcurrentHashSet;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class LocalGroupingHandler implements GroupingHandler
{
   private static Logger log = Logger.getLogger(LocalGroupingHandler.class);

   private ConcurrentHashMap<SimpleString, Object> map = new ConcurrentHashMap<SimpleString, Object>();

   private final SimpleString name;

   private final ManagementService managementService;

   private SimpleString address;

   private ScheduledExecutorService scheduledExecutor;

   private ConcurrentHashSet<SimpleString> reProposals = new ConcurrentHashSet<SimpleString>();

   public LocalGroupingHandler(final ManagementService managementService, final SimpleString name, final SimpleString address, ScheduledExecutorService scheduledExecutor)
   {
      this.managementService = managementService;
      this.name = name;
      this.address = address;
      this.scheduledExecutor = scheduledExecutor;
   }

   public SimpleString getName()
   {
      return name;
   }


   public Response propose(Proposal proposal) throws Exception
   {
      if(proposal.getProposal() == null)
      {
         Object original = map.get(proposal.getProposalType());
         return original == null?null:new Response(proposal.getProposalType(), original);
      }
      Response response = new Response(proposal.getProposalType(), proposal.getProposal());
      if (map.putIfAbsent(response.getResponseType(), response.getChosen()) == null)
      {
         return response;
      }
      else
      {
         return new Response(proposal.getProposalType(), proposal.getProposal(), map.get(proposal.getProposalType()));
      }
   }

   public void proposed(Response response) throws Exception
   {
   }

   public void send(Response response, int distance) throws Exception
   {
      Object value = response.getAlternative() != null ? response.getAlternative() : response.getOriginal();
      TypedProperties props = new TypedProperties();
      props.putStringProperty(ManagementHelper.HDR_PROPOSAL_TYPE, response.getResponseType());
      props.putStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE, (SimpleString)response.getOriginal());
      props.putStringProperty(ManagementHelper.HDR_PROPOSAL_ALT_VALUE, (SimpleString)response.getAlternative());
      props.putIntProperty(ManagementHelper.HDR_BINDING_TYPE, BindingType.LOCAL_QUEUE_INDEX);
      props.putStringProperty(ManagementHelper.HDR_ADDRESS, address);
      props.putIntProperty(ManagementHelper.HDR_DISTANCE, distance);
      Notification notification = new Notification(null, NotificationType.PROPOSAL_RESPONSE, props);
      managementService.sendNotification(notification);
   }

   public Response receive(Proposal proposal, int distance) throws Exception
   {
      return propose(proposal);
   }

   public Response rePropose(final Proposal proposal) throws Exception
   {
      if(reProposals.addIfAbsent(proposal.getProposalType()))
      {
         Response response = new Response(proposal.getProposalType(), proposal.getProposal());
         map.replace(proposal.getProposalType(), response);
         send(response, 0);
         scheduledExecutor.schedule(new Runnable()
         {
            public void run()
            {
               reProposals.remove(proposal.getProposalType());
            }
         }, 2000, TimeUnit.MILLISECONDS);
         return response;
      }
      else
      {
         return new Response(proposal.getProposalType(), map.get(proposal.getProposalType()));
      }
   }
}

