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

package org.hornetq.tests.integration.persistence;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.GroupingInfo;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.Queue;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A DeleteMessagesRestartTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Mar 2, 2009 10:14:38 AM
 *
 *
 */
public class RestartSMTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(RestartSMTest.class);
                                                      
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testRestartStorageManager() throws Exception
   {
      File testdir = new File(getTestDir());
      deleteDirectory(testdir);

      Configuration configuration = createDefaultConfig();

      configuration.start();

      configuration.setJournalType(JournalType.ASYNCIO);

      final JournalStorageManager journal = new JournalStorageManager(configuration, Executors.newCachedThreadPool());
      try
      {

         journal.start();

         List<QueueBindingInfo> queueBindingInfos = new ArrayList<QueueBindingInfo>();

         journal.loadBindingJournal(queueBindingInfos, new ArrayList<GroupingInfo>());

         Map<Long, Queue> queues = new HashMap<Long, Queue>();

         journal.loadMessageJournal(null, null, queues, null);

         journal.stop();

         deleteDirectory(testdir);

         journal.start();

         queues = new HashMap<Long, Queue>();

         journal.loadMessageJournal(null, null, queues, null);

         queueBindingInfos = new ArrayList<QueueBindingInfo>();

         journal.loadBindingJournal(queueBindingInfos, new ArrayList<GroupingInfo>());

         journal.start();
      }
      finally
      {

         try
         {
            journal.stop();
         }
         catch (Exception ex)
         {
            log.warn(ex.getMessage(), ex);
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
