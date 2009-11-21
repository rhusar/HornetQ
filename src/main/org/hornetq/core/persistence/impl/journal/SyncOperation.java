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

package org.hornetq.core.persistence.impl.journal;

import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.persistence.OperationContext;

/**
 * A SyncOperation
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class SyncOperation implements OperationContext
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   OperationContext ctx;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public SyncOperation (OperationContext ctx)
   {
      this.ctx = ctx;
   }

   // Public --------------------------------------------------------

   /**
    * 
    * @see org.hornetq.core.persistence.OperationContext#complete()
    */
   public void complete()
   {
      ctx.complete();
   }

   /**
    * 
    * @see org.hornetq.core.asyncio.AIOCallback#done()
    */
   public void done()
   {
      ctx.done();
   }

   /**
    * @param runnable
    * @see org.hornetq.core.persistence.OperationContext#executeOnCompletion(org.hornetq.core.journal.IOAsyncTask)
    */
   public void executeOnCompletion(IOAsyncTask runnable)
   {
      ctx.executeOnCompletion(runnable);
   }

   /**
    * @return
    * @see org.hornetq.core.persistence.OperationContext#hasReplication()
    */
   public boolean hasReplication()
   {
      return ctx.hasReplication();
   }

   /**
    * @return
    * @see org.hornetq.core.persistence.OperationContext#isSync()
    */
   public boolean isSync()
   {
      return true;
   }

   /**
    * 
    * @see org.hornetq.core.journal.IOCompletion#lineUp()
    */
   public void lineUp()
   {
      ctx.lineUp();
   }

   /**
    * @param errorCode
    * @param errorMessage
    * @see org.hornetq.core.asyncio.AIOCallback#onError(int, java.lang.String)
    */
   public void onError(int errorCode, String errorMessage)
   {
      ctx.onError(errorCode, errorMessage);
   }

   /**
    * 
    * @see org.hornetq.core.persistence.OperationContext#replicationDone()
    */
   public void replicationDone()
   {
      ctx.replicationDone();
   }

   /**
    * 
    * @see org.hornetq.core.persistence.OperationContext#replicationLineUp()
    */
   public void replicationLineUp()
   {
      ctx.replicationLineUp();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
