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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.persistence.OperationContext;

/**
 * A ReplicationToken
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
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

   private List<TaskHolder> tasks;

   private volatile int storeLineUp = 0;

   private volatile int replicationLineUp = 0;

   private int minimalStore = Integer.MAX_VALUE;

   private int minimalReplicated = Integer.MAX_VALUE;

   private int stored = 0;

   private int replicated = 0;

   private boolean empty = false;

   /**
    * @param executor
    */
   public OperationContextImpl()
   {
      super();
   }

   /** To be called by the replication manager, when new replication is added to the queue */
   public void lineUp()
   {
      storeLineUp++;
   }

   public void replicationLineUp()
   {
      replicationLineUp++;
   }

   public synchronized void replicationDone()
   {
      replicated++;
      checkTasks();
   }

   public boolean hasReplication()
   {
      return replicationLineUp > 0;
   }

   /** You may have several actions to be done after a replication operation is completed. */
   public synchronized void executeOnCompletion(IOAsyncTask completion)
   {
      if (tasks == null)
      {
         tasks = new LinkedList<TaskHolder>();
         minimalReplicated = replicationLineUp;
         minimalStore = storeLineUp;
      }

      if (replicationLineUp == replicated && storeLineUp == stored)
      {
         completion.done();
      }
      else
      {
         tasks.add(new TaskHolder(completion));
      }
   }

   /** To be called by the storage manager, when data is confirmed on the channel */
   public synchronized void done()
   {
      stored++;
      checkTasks();
   }

   private void checkTasks()
   {
      if (stored >= minimalStore && replicated >= minimalReplicated)
      {
         Iterator<TaskHolder> iter = tasks.iterator();
         while (iter.hasNext())
         {
            TaskHolder holder = iter.next();
            if (!holder.executed && stored >= holder.storeLined && replicated >= holder.replicationLined)
            {
               holder.executed = true;
               holder.task.done();
               iter.remove();
            }
         }
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationToken#complete()
    */
   public void complete()
   {
      tlContext.set(null);
   }
   
   public boolean isSync()
   {
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.asyncio.AIOCallback#onError(int, java.lang.String)
    */
   public void onError(int errorCode, String errorMessage)
   {
      if (tasks != null)
      {
         for (TaskHolder run : tasks)
         {
            run.task.onError(errorCode, errorMessage);
         }
      }
   }

   class TaskHolder
   {
      int storeLined;

      int replicationLined;

      boolean executed;

      IOAsyncTask task;

      TaskHolder(IOAsyncTask task)
      {
         this.storeLined = storeLineUp;
         this.replicationLined = replicationLineUp;
         this.task = task;
      }
   }

}
