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

package org.hornetq.core.paging.cursor;

import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;

/**
 * A InternalReference
 *
 * @author clebert
 *
 *
 */
public class PagedReferenceImpl implements PagedReference
{

   private static final long serialVersionUID = -8640232251318264710L;

   private final PagePosition position;

   private final PagedMessage message;
   
   private final PageSubscription subscription;

   public ServerMessage getMessage()
   {
      return message.getMessage();
   }

   public PagedMessage getPagedMessage()
   {
      return message;
   }

   public PagePosition getPosition()
   {
      return position;
   }

   public PagedReferenceImpl(final PagePosition position, final PagedMessage message, final PageSubscription subscription)
   {
      this.position = position;
      this.message = message;
      this.subscription = subscription;
   }

   public boolean isPaged()
   {
      return true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#copy(org.hornetq.core.server.Queue)
    */
   public MessageReference copy(final Queue queue)
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#getScheduledDeliveryTime()
    */
   public long getScheduledDeliveryTime()
   {
      // TODO Auto-generated method stub
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#setScheduledDeliveryTime(long)
    */
   public void setScheduledDeliveryTime(final long scheduledDeliveryTime)
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#getDeliveryCount()
    */
   public int getDeliveryCount()
   {
      // TODO Auto-generated method stub
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#setDeliveryCount(int)
    */
   public void setDeliveryCount(final int deliveryCount)
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#incrementDeliveryCount()
    */
   public void incrementDeliveryCount()
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#decrementDeliveryCount()
    */
   public void decrementDeliveryCount()
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#getQueue()
    */
   public Queue getQueue()
   {
      return subscription.getQueue();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#handled()
    */
   public void handled()
   {
      getQueue().referenceHandled();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#acknowledge()
    */
   public void acknowledge() throws Exception
   {
      subscription.ack(this);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#acknowledge(org.hornetq.core.transaction.Transaction)
    */
   public void acknowledge(final Transaction tx) throws Exception
   {
      subscription.ackTx(tx, this);
   }
}
