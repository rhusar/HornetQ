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
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;

import java.util.logging.Logger;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class RemoteGroupingHandler implements GroupingHandler
{
   private static Logger log = Logger.getLogger(RemoteGroupingHandler.class.getName());

   private final SimpleString name;

   private final ManagementService managementService;

   private final SimpleString address;

   private Map<SimpleString, Response> responses = new HashMap<SimpleString, Response>();

   private final Lock lock = new ReentrantLock();

   private final Condition sendCondition = lock.newCondition();

   private int waitTime = 1000;

   public RemoteGroupingHandler(final ManagementService managementService, final SimpleString name, final SimpleString address)
   {
      this.name = name;
      this.address = address;
      this.managementService = managementService;
   }

   public SimpleString getName()
   {
      return name;
   }

   public Response propose(final Proposal proposal) throws Exception
   {
      Response response = responses.get(proposal.getProposalType());
      if( response != null)
      {
         return response;
      }
      if (proposal.getProposal() == null)
      {
         return null;
      }
      try
      {
         lock.lock();
         TypedProperties props = new TypedProperties();
         props.putStringProperty(ManagementHelper.HDR_PROPOSAL_TYPE, proposal.getProposalType());
         props.putStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE, (SimpleString)proposal.getProposal());
         props.putIntProperty(ManagementHelper.HDR_BINDING_TYPE, BindingType.LOCAL_QUEUE_INDEX);
         props.putStringProperty(ManagementHelper.HDR_ADDRESS, address);
         props.putIntProperty(ManagementHelper.HDR_DISTANCE, 0);
         Notification notification = new Notification(null, NotificationType.PROPOSAL, props);
         managementService.sendNotification(notification);
         sendCondition.await(waitTime, TimeUnit.MILLISECONDS);
         response = responses.get(proposal.getProposalType());
      }
      finally
      {
         lock.unlock();
      }
      if(response == null)
      {
         throw new IllegalStateException("no response received from group handler for " + proposal.getProposalType());
      }
      return response;
   }

   public void proposed(Response response) throws Exception
   {
      try
      {
         lock.lock();
         responses.put(response.getResponseType(), response);
         sendCondition.signal();
      }
      finally
      {
         lock.unlock();
      }
   }

   public Response receive(Proposal proposal, int distance) throws Exception
   {
      TypedProperties props = new TypedProperties();
      props.putStringProperty(ManagementHelper.HDR_PROPOSAL_TYPE, proposal.getProposalType());
      props.putStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE, (SimpleString)proposal.getProposal());
      props.putIntProperty(ManagementHelper.HDR_BINDING_TYPE, BindingType.LOCAL_QUEUE_INDEX);
      props.putStringProperty(ManagementHelper.HDR_ADDRESS, address);
      props.putIntProperty(ManagementHelper.HDR_DISTANCE, distance);
      Notification notification = new Notification(null, NotificationType.PROPOSAL, props);
      managementService.sendNotification(notification);
      return null;
   }

   public void send(Response response, int distance) throws Exception
   {
   }

}

