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

package org.hornetq.tests.unit.core.config.impl;

import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomInt;
import static org.hornetq.tests.util.RandomUtil.randomLong;
import static org.hornetq.tests.util.RandomUtil.randomString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A ConfigurationImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ConfigurationImplTest extends UnitTestCase
{
   protected Configuration conf;
         
   public void testDefaults()
   {      
      assertEquals(ConfigurationImpl.DEFAULT_CLUSTERED, conf.isClustered());
      assertEquals(ConfigurationImpl.DEFAULT_BACKUP, conf.isBackup());
      assertEquals(ConfigurationImpl.DEFAULT_SHARED_STORE, conf.isSharedStore());
      assertEquals(ConfigurationImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, conf.getScheduledThreadPoolMaxSize());
      assertEquals(ConfigurationImpl.DEFAULT_SECURITY_INVALIDATION_INTERVAL, conf.getSecurityInvalidationInterval());
      assertEquals(ConfigurationImpl.DEFAULT_SECURITY_ENABLED, conf.isSecurityEnabled());     
      assertEquals(ConfigurationImpl.DEFAULT_BINDINGS_DIRECTORY, conf.getBindingsDirectory());
      assertEquals(ConfigurationImpl.DEFAULT_CREATE_BINDINGS_DIR, conf.isCreateBindingsDir());
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_DIR, conf.getJournalDirectory());
      assertEquals(ConfigurationImpl.DEFAULT_CREATE_JOURNAL_DIR, conf.isCreateJournalDir());
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_TYPE, conf.getJournalType());
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_SYNC_TRANSACTIONAL, conf.isJournalSyncTransactional());
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL, conf.isJournalSyncNonTransactional());
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE, conf.getJournalFileSize());
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MIN_FILES, conf.getJournalMinFiles());      
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MAX_AIO, conf.getJournalMaxAIO());
      assertEquals(ConfigurationImpl.DEFAULT_WILDCARD_ROUTING_ENABLED, conf.isWildcardRoutingEnabled());
      assertEquals(ConfigurationImpl.DEFAULT_TRANSACTION_TIMEOUT, conf.getTransactionTimeout());
      assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD, conf.getMessageExpiryScanPeriod());
      assertEquals(ConfigurationImpl.DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY, conf.getMessageExpiryThreadPriority());
      assertEquals(ConfigurationImpl.DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD, conf.getTransactionTimeoutScanPeriod());
      assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS, conf.getManagementAddress());
      assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS, conf.getManagementNotificationAddress());
      assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_USER, conf.getManagementClusterUser());
      assertEquals(ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_PASSWORD, conf.getManagementClusterPassword());
      assertEquals(ConfigurationImpl.DEFAULT_PERSISTENCE_ENABLED, conf.isPersistenceEnabled());
      assertEquals(ConfigurationImpl.DEFAULT_FILE_DEPLOYMENT_ENABLED, conf.isFileDeploymentEnabled());
   }
   
   public void testSetGetAttributes()
   {
      for (int j = 0; j < 100; j++)
      {         
         boolean b = randomBoolean();
         conf.setClustered(b);
         assertEquals(b, conf.isClustered());
         
         b = randomBoolean();
         conf.setBackup(b);
         assertEquals(b, conf.isBackup());
         
         b = randomBoolean();
         conf.setSharedStore(b);
         assertEquals(b, conf.isSharedStore());
         
         int i = randomInt();
         conf.setScheduledThreadPoolMaxSize(i);
         assertEquals(i, conf.getScheduledThreadPoolMaxSize());
                  
         long l = randomLong();
         conf.setSecurityInvalidationInterval(l);
         assertEquals(l, conf.getSecurityInvalidationInterval());
         
         b = randomBoolean();
         conf.setSecurityEnabled(b);
         assertEquals(b, conf.isSecurityEnabled());
     
         String s = randomString();
         conf.setBindingsDirectory(s);
         assertEquals(s, conf.getBindingsDirectory());
         
         b = randomBoolean();
         conf.setCreateBindingsDir(b);
         assertEquals(b, conf.isCreateBindingsDir());
         
         s = randomString();
         conf.setJournalDirectory(s);
         assertEquals(s, conf.getJournalDirectory());
         
         b = randomBoolean();
         conf.setCreateJournalDir(b);
         assertEquals(b, conf.isCreateJournalDir());
         
         i = randomInt() % 2;
         JournalType journal = i == 0 ? JournalType.ASYNCIO : JournalType.NIO;
         conf.setJournalType(journal);
         assertEquals(journal, conf.getJournalType());
         
         b = randomBoolean();
         conf.setJournalSyncTransactional(b);
         assertEquals(b, conf.isJournalSyncTransactional());
         
         b = randomBoolean();
         conf.setJournalSyncNonTransactional(b);
         assertEquals(b, conf.isJournalSyncNonTransactional());
         
         i = randomInt();
         conf.setJournalFileSize(i);
         assertEquals(i, conf.getJournalFileSize());
         
         i = randomInt();
         conf.setJournalMinFiles(i);
         assertEquals(i, conf.getJournalMinFiles());
         
         i = randomInt();
         conf.setJournalMaxAIO(i);
         assertEquals(i, conf.getJournalMaxAIO());  
         
         s = randomString();
         conf.setManagementAddress(new SimpleString(s));
         assertEquals(s, conf.getManagementAddress().toString());

         i = randomInt();

         conf.setMessageExpiryThreadPriority(i);
         assertEquals(i, conf.getMessageExpiryThreadPriority());

         l = randomLong();
         conf.setMessageExpiryScanPeriod(l);
         assertEquals(l, conf.getMessageExpiryScanPeriod());
      }
   }
   
   public void testGetSetInterceptors()
   {
      final String name1 = "uqwyuqywuy";
      final String name2 = "yugyugyguyg";
      
      conf.getInterceptorClassNames().add(name1);
      conf.getInterceptorClassNames().add(name2);
      
      assertTrue(conf.getInterceptorClassNames().contains(name1));
      assertTrue(conf.getInterceptorClassNames().contains(name2));
      assertFalse(conf.getInterceptorClassNames().contains("iijij"));
   }
   
   public void testSerialize() throws Exception
   {
      boolean b = randomBoolean();
      conf.setClustered(b);
      
      b = randomBoolean();
      conf.setBackup(b);
      
      b = randomBoolean();
      conf.setSharedStore(b);
            
      int i = randomInt();
      conf.setScheduledThreadPoolMaxSize(i);
         
      long l = randomLong();
      conf.setSecurityInvalidationInterval(l);

      b = randomBoolean();
      conf.setSecurityEnabled(b);
        
      String s = randomString();
      conf.setBindingsDirectory(s);

      b = randomBoolean();
      conf.setCreateBindingsDir(b);

      s = randomString();
      conf.setJournalDirectory(s);
   
      b = randomBoolean();
      conf.setCreateJournalDir(b);
   
      i = randomInt() % 2;
      JournalType journal = i == 0 ? JournalType.ASYNCIO : JournalType.NIO;
      conf.setJournalType(journal);
   
      b = randomBoolean();
      conf.setJournalSyncTransactional(b);
    
      b = randomBoolean();
      conf.setJournalSyncNonTransactional(b);

      i = randomInt();
      conf.setJournalFileSize(i);
 
      i = randomInt();
      conf.setJournalMinFiles(i);
 
      i = randomInt();
      conf.setJournalMaxAIO(i);
      
      s = randomString();
      conf.setManagementAddress(new SimpleString(s));
  
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(conf);
      oos.flush();
      
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(bais);
      Configuration conf2 = (Configuration)ois.readObject();
      
      assertTrue(conf.equals(conf2));      
   }
   
   // Protected ----------------------------------------------------------------------------------------
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      conf = createConfiguration();
   }
   
   protected Configuration createConfiguration() throws Exception
   {
      return new ConfigurationImpl();
   }
   
   // Private --------------------------------------------------------------------------------------------
    
}
