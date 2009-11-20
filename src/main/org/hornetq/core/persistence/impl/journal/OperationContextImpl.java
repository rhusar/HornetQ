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

import java.util.LinkedList;
import java.util.List;

import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.persistence.OperationContext;

/**
 * A ReplicationToken
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * TODO: Maybe I should move this to persistence.journal. I need to check a few dependencies first.
 *
 */
public class OperationContextImpl implements OperationContext
{
   private static final ThreadLocal<OperationContext> tlContext = new ThreadLocal<OperationContext>();

   public static OperationContext getContext()
   {
      OperationContext token = tlContext.get();
      if (token == null)
      {
         token = new OperationContextImpl();
         tlContext.set(token);
      }
      return token;
   }

   private List<IOAsyncTask> tasks;
   
   private int storeLinedUp = 0;

   private int stored = 0;

   private boolean empty = false;

   private volatile boolean complete = false;

   /**
    * @param executor
    */
   public OperationContextImpl()
   {
      super();
   }

   /** To be called by the replication manager, when new replication is added to the queue */
   public void linedUp()
   {
      storeLinedUp++;
   }

   public boolean hasData()
   {
      return storeLinedUp > 0;
   }

   /** You may have several actions to be done after a replication operation is completed. */
   public void executeOnCompletion(IOAsyncTask completion)
   {
      if (complete)
      {
         // Sanity check, this shouldn't happen
         throw new IllegalStateException("The Replication Context is complete, and no more tasks are accepted");
      }

      if (tasks == null)
      {
         // No need to use Concurrent, we only add from a single thread.
         // We don't add any more Runnables after it is complete
         tasks = new LinkedList<IOAsyncTask>();
      }

      tasks.add(completion);
   }

   /** To be called by the storage manager, when data is confirmed on the channel */
   public synchronized void done()
   {
      if (++stored == storeLinedUp && complete)
      {
         flush();
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationToken#complete()
    */
   public synchronized void complete()
   {
      tlContext.set(null);
      complete = true;
      if (stored == storeLinedUp && complete)
      {
         flush();
      }
   }

   public synchronized void flush()
   {
      if (tasks != null)
      {
         for (IOAsyncTask run : tasks)
         {
            run.done();
         }
         tasks.clear();
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationContext#isRoundtrip()
    */
   public boolean isEmpty()
   {
      return empty;
   }

   public void setEmpty(final boolean sync)
   {
      this.empty = sync;
   }

   
   /* (non-Javadoc)
    * @see org.hornetq.core.asyncio.AIOCallback#onError(int, java.lang.String)
    */
   public void onError(int errorCode, String errorMessage)
   {
      if (tasks != null)
      {
         for (IOAsyncTask run : tasks)
         {
            run.onError(errorCode, errorMessage);
         }
      }
   }

}
