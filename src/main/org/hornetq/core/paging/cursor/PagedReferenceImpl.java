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

   private PagePosition a;
   private PagedMessage b;
   
   private Queue queue;
   
   private PageSubscription subscription;
   
   
   public ServerMessage getMessage()
   {
      return b.getMessage();
   }
   
   public PagedMessage getPagedMessage()
   {
      return b;
   }
   
   public PagePosition getPosition()
   {
      return a;
   }

   public PagedReferenceImpl(PagePosition a, PagedMessage b)
   {
      this.a = a;
      this.b = b;
   }
   
   public boolean isPaged()
   {
      return true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#copy(org.hornetq.core.server.Queue)
    */
   public MessageReference copy(Queue queue)
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
   public void setScheduledDeliveryTime(long scheduledDeliveryTime)
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
   public void setDeliveryCount(int deliveryCount)
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
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#handled()
    */
   public void handled()
   {
      // TODO Auto-generated method stub
      
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#acknowledge()
    */
   public void acknowledge() throws Exception
   {
      // TODO Auto-generated method stub
      
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#acknowledge(org.hornetq.core.transaction.Transaction)
    */
   public void acknowledge(Transaction tx) throws Exception
   {
      // TODO Auto-generated method stub
      
   }
}
