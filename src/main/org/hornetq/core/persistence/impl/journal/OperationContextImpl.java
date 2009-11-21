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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.persistence.OperationContext;

import sun.security.util.PendingException;

/**
 * 
 * This class will hold operations when there are IO operations...
 * and it will 
 * 
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class OperationContextImpl implements OperationContext
{
   private static final ThreadLocal<OperationContext> tlContext = new ThreadLocal<OperationContext>();

   public static void setInstance(OperationContext context)
   {
      tlContext.set(context);
   }
   
   public static OperationContext getInstance()
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
   
   private int errorCode = -1;
   
   private String errorMessage = null;
   
   private Executor executor;
   
   private final AtomicInteger executorsPending = new AtomicInteger(0);

   /**
    * @param executor
    */
   public OperationContextImpl()
   {
      super();
   }

   public OperationContextImpl(final Executor executor)
   {
      super();
      this.executor = executor;
   }
   
   /*
    * @see org.hornetq.core.persistence.OperationContext#reinstall()
    */
   public void reinstall()
   {
      setInstance(this);
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
   
   /** this method needs to be called before the executor became operational */
   public void setExecutor(Executor executor)
   {
      this.executor = executor;
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
   public synchronized void executeOnCompletion(final IOAsyncTask completion)
   {
      if (tasks == null)
      {
         tasks = new LinkedList<TaskHolder>();
         minimalReplicated = replicationLineUp;
         minimalStore = storeLineUp;
      }

      // On this case, we can just execute the context directly
      if (replicationLineUp == replicated && storeLineUp == stored)
      {
         if (executor != null)
         {
            // We want to avoid the executor if everything is complete...
            // However, we can't execute the context if there are executions pending
            // We need to use the executor on this case
            if (executorsPending.get() == 0)
            {
               // No need to use an executor here or a context switch
               // there are no actions pending.. hence we can just execute the task directly on the same thread
               completion.done();
            }
            else
            {
               execute(completion);
            }
         }
         else
         {
            // Execute without an executor
            completion.done();
         }
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
                
               if (executor != null)
               {
                  // If set, we use an executor to avoid the server being single threaded
                  execute(holder.task);
               }
               else
               {
                  holder.task.done();
               }
               
               iter.remove();
            }
            else
            {
               // The actions need to be done in order...
               // so it must achieve both conditions before we can proceed to more tasks
               break;
            }
         }
      }
   }

   /**
    * @param holder
    */
   private void execute(final IOAsyncTask task)
   {
      executorsPending.incrementAndGet();
      executor.execute(new Runnable()
      {
         public void run()
         {
            task.done();
            executorsPending.decrementAndGet();
         }
      });
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationToken#complete()
    */
   public void complete()
   {
      tlContext.set(null);
      
      // TODO: test and fix exceptions on the Context
      if (tasks != null && errorMessage != null)
      {
         for (TaskHolder run : tasks)
         {
            run.task.onError(errorCode, errorMessage);
         }
      }
      
      // We hold errors until the complete is set, or the callbacks will never get informed
      errorCode = -1;
      errorMessage = null;
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
      this.errorCode = errorCode;
      this.errorMessage = errorMessage;
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
