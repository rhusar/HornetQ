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

package org.hornetq.core.config.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.BridgeConfiguration;
import org.hornetq.core.config.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.config.cluster.ClusterConnectionConfiguration;
import org.hornetq.core.config.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.core.config.cluster.QueueConfiguration;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ConfigurationImpl implements Configuration
{
   // Constants ------------------------------------------------------------------------------

   private static final long serialVersionUID = 4077088945050267843L;

   public static final boolean DEFAULT_CLUSTERED = false;

   public static final boolean DEFAULT_PERSIST_DELIVERY_COUNT_BEFORE_DELIVERY = false;

   public static final boolean DEFAULT_BACKUP = false;

   public static final boolean DEFAULT_FILE_DEPLOYMENT_ENABLED = false;

   public static final boolean DEFAULT_PERSISTENCE_ENABLED = true;

   public static final long DEFAULT_FILE_DEPLOYER_SCAN_PERIOD = 5000;

   public static final long DEFAULT_QUEUE_ACTIVATION_TIMEOUT = 30000;

   public static final int DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE = 5;

   public static final int DEFAULT_THREAD_POOL_MAX_SIZE = -1;

   public static final long DEFAULT_SECURITY_INVALIDATION_INTERVAL = 10000;

   public static final boolean DEFAULT_SECURITY_ENABLED = true;

   public static final boolean DEFAULT_JMX_MANAGEMENT_ENABLED = true;

   public static final long DEFAULT_CONNECTION_TTL_OVERRIDE = -1;
   
   public static final boolean DEFAULT_ASYNC_CONNECTION_EXECUTION_ENABLED = true;

   public static final String DEFAULT_BINDINGS_DIRECTORY = "data/bindings";

   public static final boolean DEFAULT_CREATE_BINDINGS_DIR = true;

   public static final String DEFAULT_JOURNAL_DIR = "data/journal";

   public static final String DEFAULT_PAGING_DIR = "data/paging";

   public static final String DEFAULT_LARGE_MESSAGES_DIR = "data/largemessages";

   public static final boolean DEFAULT_CREATE_JOURNAL_DIR = true;

   public static final JournalType DEFAULT_JOURNAL_TYPE = JournalType.ASYNCIO;

   public static final boolean DEFAULT_JOURNAL_SYNC_TRANSACTIONAL = true;

   public static final boolean DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL = false;

   public static final int DEFAULT_JOURNAL_FILE_SIZE = 10485760;
   
   public static final int DEFAULT_JOURNAL_COMPACT_MIN_FILES = 10;
   
   public static final int DEFAULT_JOURNAL_COMPACT_PERCENTAGE = 30;

   public static final int DEFAULT_JOURNAL_MIN_FILES = 2;

   public static final int DEFAULT_JOURNAL_MAX_AIO = 500;
   
   public static final boolean DEFAULT_JOURNAL_AIO_FLUSH_SYNC = false;
   
   public static final int DEFAULT_JOURNAL_AIO_BUFFER_TIMEOUT = 20000;
   
   public static final int DEFAULT_JOURNAL_AIO_BUFFER_SIZE = 128 * 1024;
   
   public static final boolean DEFAULT_JOURNAL_LOG_WRITE_RATE = false;
   
   public static final int DEFAULT_JOURNAL_PERF_BLAST_PAGES = -1;

   public static final boolean DEFAULT_WILDCARD_ROUTING_ENABLED = true;

   public static final boolean DEFAULT_MESSAGE_COUNTER_ENABLED = false;

   public static final long DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD = 10000;

   public static final int DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY = 10;

   public static final long DEFAULT_TRANSACTION_TIMEOUT = 60000;

   public static final long DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD = 1000;

   public static final SimpleString DEFAULT_MANAGEMENT_ADDRESS = new SimpleString("hornetq.management");

   public static final SimpleString DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS = new SimpleString("hornetq.notifications");

   public static final String DEFAULT_MANAGEMENT_CLUSTER_USER = "HORNETQ.MANAGEMENT.ADMIN.USER";

   public static final String DEFAULT_MANAGEMENT_CLUSTER_PASSWORD = "CHANGE ME!!";

   public static final long DEFAULT_MANAGEMENT_REQUEST_TIMEOUT = 5000;

   public static final long DEFAULT_BROADCAST_PERIOD = 1000;

   public static final long DEFAULT_BROADCAST_REFRESH_TIMEOUT = 10000;

   public static final long DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD = 30000;

   public static final int DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY = 3;

   public static final int DEFAULT_ID_CACHE_SIZE = 2000;

   public static final boolean DEFAULT_PERSIST_ID_CACHE = true;

   public static final boolean DEFAULT_CLUSTER_DUPLICATE_DETECTION = true;

   public static final boolean DEFAULT_CLUSTER_FORWARD_WHEN_NO_CONSUMERS = false;

   public static final int DEFAULT_CLUSTER_MAX_HOPS = 1;

   public static final int DEFAULT_CLUSTER_RETRY_INTERVAL = 500;

   public static final boolean DEFAULT_DIVERT_EXCLUSIVE = false;

   public static final boolean DEFAULT_BRIDGE_DUPLICATE_DETECTION = true;

   public static final int DEFAULT_BRIDGE_RECONNECT_ATTEMPTS = -1;

   public static final long DEFAULT_SERVER_DUMP_INTERVAL = -1;

   // Attributes -----------------------------------------------------------------------------

   protected boolean clustered = DEFAULT_CLUSTERED;

   protected boolean backup = DEFAULT_BACKUP;

   protected boolean fileDeploymentEnabled = DEFAULT_FILE_DEPLOYMENT_ENABLED;

   protected boolean persistenceEnabled = DEFAULT_PERSISTENCE_ENABLED;

   protected long fileDeploymentScanPeriod = DEFAULT_FILE_DEPLOYER_SCAN_PERIOD;

   protected boolean persistDeliveryCountBeforeDelivery = DEFAULT_PERSIST_DELIVERY_COUNT_BEFORE_DELIVERY;

   protected long queueActivationTimeout = DEFAULT_QUEUE_ACTIVATION_TIMEOUT;

   protected int scheduledThreadPoolMaxSize = DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;

   protected int threadPoolMaxSize = DEFAULT_THREAD_POOL_MAX_SIZE;

   protected long securityInvalidationInterval = DEFAULT_SECURITY_INVALIDATION_INTERVAL;

   protected boolean securityEnabled = DEFAULT_SECURITY_ENABLED;

   protected boolean jmxManagementEnabled = DEFAULT_JMX_MANAGEMENT_ENABLED;

   protected long connectionTTLOverride = DEFAULT_CONNECTION_TTL_OVERRIDE;
   
   protected boolean asyncConnectionExecutionEnabled = DEFAULT_ASYNC_CONNECTION_EXECUTION_ENABLED;
   
   protected long messageExpiryScanPeriod = DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD;

   protected int messageExpiryThreadPriority = DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY;

   protected int idCacheSize = DEFAULT_ID_CACHE_SIZE;

   protected boolean persistIDCache = DEFAULT_PERSIST_ID_CACHE;

   protected List<String> interceptorClassNames = new ArrayList<String>();

   protected Map<String, TransportConfiguration> connectorConfigs = new HashMap<String, TransportConfiguration>();

   protected Set<TransportConfiguration> acceptorConfigs = new HashSet<TransportConfiguration>();

   protected String backupConnectorName;

   protected List<BridgeConfiguration> bridgeConfigurations = new ArrayList<BridgeConfiguration>();

   protected List<DivertConfiguration> divertConfigurations = new ArrayList<DivertConfiguration>();

   protected List<ClusterConnectionConfiguration> clusterConfigurations = new ArrayList<ClusterConnectionConfiguration>();

   protected List<QueueConfiguration> queueConfigurations = new ArrayList<QueueConfiguration>();

   protected List<BroadcastGroupConfiguration> broadcastGroupConfigurations = new ArrayList<BroadcastGroupConfiguration>();

   protected Map<String, DiscoveryGroupConfiguration> discoveryGroupConfigurations = new LinkedHashMap<String, DiscoveryGroupConfiguration>();

   protected List<GroupingHandlerConfiguration> groupingHandlerConfiguration = new ArrayList<GroupingHandlerConfiguration>();

   // Paging related attributes ------------------------------------------------------------

   protected String pagingDirectory = DEFAULT_PAGING_DIR;

   // File related attributes -----------------------------------------------------------

   protected String largeMessagesDirectory = DEFAULT_LARGE_MESSAGES_DIR;

   protected String bindingsDirectory = DEFAULT_BINDINGS_DIRECTORY;

   protected boolean createBindingsDir = DEFAULT_CREATE_BINDINGS_DIR;

   protected String journalDirectory = DEFAULT_JOURNAL_DIR;

   protected boolean createJournalDir = DEFAULT_CREATE_JOURNAL_DIR;

   public JournalType journalType = DEFAULT_JOURNAL_TYPE;

   protected boolean journalSyncTransactional = DEFAULT_JOURNAL_SYNC_TRANSACTIONAL;

   protected boolean journalSyncNonTransactional = DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL;

   
   protected int journalCompactMinFiles = DEFAULT_JOURNAL_COMPACT_MIN_FILES;
   
   protected int journalCompactPercentage = DEFAULT_JOURNAL_COMPACT_PERCENTAGE;
   
   protected int journalFileSize = DEFAULT_JOURNAL_FILE_SIZE;

   protected int journalMinFiles = DEFAULT_JOURNAL_MIN_FILES;

   protected int journalMaxAIO = DEFAULT_JOURNAL_MAX_AIO;
   
   protected boolean journalAIOFlushSync = DEFAULT_JOURNAL_AIO_FLUSH_SYNC;
   
   protected int journalAIOBufferTimeout = DEFAULT_JOURNAL_AIO_BUFFER_TIMEOUT;
   
   protected int journalAIOBufferSize = DEFAULT_JOURNAL_AIO_BUFFER_SIZE;
   
   protected boolean logJournalWriteRate = DEFAULT_JOURNAL_LOG_WRITE_RATE;
   
   protected int journalPerfBlastPages = DEFAULT_JOURNAL_PERF_BLAST_PAGES;

   protected boolean wildcardRoutingEnabled = DEFAULT_WILDCARD_ROUTING_ENABLED;

   protected boolean messageCounterEnabled = DEFAULT_MESSAGE_COUNTER_ENABLED;

   protected long messageCounterSamplePeriod = DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD;

   protected int messageCounterMaxDayHistory = DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY;

   protected long transactionTimeout = DEFAULT_TRANSACTION_TIMEOUT;

   protected long transactionTimeoutScanPeriod = DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD;

   protected SimpleString managementAddress = DEFAULT_MANAGEMENT_ADDRESS;

   protected SimpleString managementNotificationAddress = DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS;

   protected String managementClusterUser = DEFAULT_MANAGEMENT_CLUSTER_USER;

   protected String managementClusterPassword = DEFAULT_MANAGEMENT_CLUSTER_PASSWORD;

   protected long managementRequestTimeout = DEFAULT_MANAGEMENT_REQUEST_TIMEOUT;

   protected long serverDumpInterval = DEFAULT_SERVER_DUMP_INTERVAL;


   // Public -------------------------------------------------------------------------

   public void start() throws Exception
   {
   }

   public void stop() throws Exception
   {
   }

   public boolean isStarted()
   {
      return true;
   }

   public boolean isClustered()
   {
      return clustered;
   }

   public void setClustered(final boolean clustered)
   {
      this.clustered = clustered;
   }

   public boolean isBackup()
   {
      return backup;
   }

   public boolean isFileDeploymentEnabled()
   {
      return fileDeploymentEnabled;
   }

   public void setFileDeploymentEnabled(final boolean enable)
   {
      fileDeploymentEnabled = enable;
   }

   public boolean isPersistenceEnabled()
   {
      return this.persistenceEnabled;
   }

   public void setPersistenceEnabled(boolean enable)
   {
      this.persistenceEnabled = enable;
   }

   public long getFileDeployerScanPeriod()
   {
      return fileDeploymentScanPeriod;
   }

   public void setFileDeployerScanPeriod(final long period)
   {
      fileDeploymentScanPeriod = period;
   }

   /**
    * @return the persistDeliveryCountBeforeDelivery
    */
   public boolean isPersistDeliveryCountBeforeDelivery()
   {
      return persistDeliveryCountBeforeDelivery;
   }

   /**
    * @param strictJMS the strictJMS to set
    */
   public void setPersistDeliveryCountBeforeDelivery(final boolean persistDeliveryCountBeforeDelivery)
   {
      this.persistDeliveryCountBeforeDelivery = persistDeliveryCountBeforeDelivery;
   }

   public void setBackup(final boolean backup)
   {
      this.backup = backup;
   }

   public long getQueueActivationTimeout()
   {
      return queueActivationTimeout;
   }

   public void setQueueActivationTimeout(final long timeout)
   {
      queueActivationTimeout = timeout;
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return scheduledThreadPoolMaxSize;
   }

   public void setScheduledThreadPoolMaxSize(final int maxSize)
   {
      scheduledThreadPoolMaxSize = maxSize;
   }

   public int getThreadPoolMaxSize()
   {
      return threadPoolMaxSize;
   }

   public void setThreadPoolMaxSize(final int maxSize)
   {
      threadPoolMaxSize = maxSize;
   }

   public long getSecurityInvalidationInterval()
   {
      return securityInvalidationInterval;
   }

   public void setSecurityInvalidationInterval(final long interval)
   {
      securityInvalidationInterval = interval;
   }

   public long getConnectionTTLOverride()
   {
      return connectionTTLOverride;
   }

   public void setConnectionTTLOverride(final long ttl)
   {
      connectionTTLOverride = ttl;
   }
   
   public boolean isAsyncConnectionExecutionEnabled()
   {
      return asyncConnectionExecutionEnabled;
   }
   
   public void setEnabledAsyncConnectionExecution(final boolean enabled)
   {
      asyncConnectionExecutionEnabled = enabled;
   }

   public List<String> getInterceptorClassNames()
   {
      return interceptorClassNames;
   }

   public void setInterceptorClassNames(final List<String> interceptors)
   {
      interceptorClassNames = interceptors;
   }

   public Set<TransportConfiguration> getAcceptorConfigurations()
   {
      return acceptorConfigs;
   }

   public void setAcceptorConfigurations(final Set<TransportConfiguration> infos)
   {
      acceptorConfigs = infos;
   }

   public Map<String, TransportConfiguration> getConnectorConfigurations()
   {
      return connectorConfigs;
   }

   public void setConnectorConfigurations(final Map<String, TransportConfiguration> infos)
   {
      connectorConfigs = infos;
   }

   public String getBackupConnectorName()
   {
      return backupConnectorName;
   }

   public void setBackupConnectorName(final String backupConnectorName)
   {
      this.backupConnectorName = backupConnectorName;
   }

   public List<GroupingHandlerConfiguration> getGroupingHandlerConfigurations()
   {
      return groupingHandlerConfiguration;
   }

   public void setGroupingHandlerConfigurationConfigurations(List<GroupingHandlerConfiguration> groupingHandlerConfiguration)
   {
      this.groupingHandlerConfiguration = groupingHandlerConfiguration;
   }


   public List<BridgeConfiguration> getBridgeConfigurations()
   {
      return bridgeConfigurations;
   }

   public void setBridgeConfigurations(final List<BridgeConfiguration> configs)
   {
      bridgeConfigurations = configs;
   }

   public List<BroadcastGroupConfiguration> getBroadcastGroupConfigurations()
   {
      return broadcastGroupConfigurations;
   }

   public void setBroadcastGroupConfigurations(final List<BroadcastGroupConfiguration> configs)
   {
      broadcastGroupConfigurations = configs;
   }

   public List<ClusterConnectionConfiguration> getClusterConfigurations()
   {
      return clusterConfigurations;
   }

   public void setClusterConfigurations(final List<ClusterConnectionConfiguration> configs)
   {
      clusterConfigurations = configs;
   }

   public List<DivertConfiguration> getDivertConfigurations()
   {
      return divertConfigurations;
   }

   public void setDivertConfigurations(final List<DivertConfiguration> configs)
   {
      divertConfigurations = configs;
   }

   public List<QueueConfiguration> getQueueConfigurations()
   {
      return queueConfigurations;
   }

   public void setQueueConfigurations(final List<QueueConfiguration> configs)
   {
      queueConfigurations = configs;
   }

   public Map<String, DiscoveryGroupConfiguration> getDiscoveryGroupConfigurations()
   {
      return discoveryGroupConfigurations;
   }

   public void setDiscoveryGroupConfigurations(final Map<String, DiscoveryGroupConfiguration> discoveryGroupConfigurations)
   {
      this.discoveryGroupConfigurations = discoveryGroupConfigurations;
   }

   public int getIDCacheSize()
   {
      return idCacheSize;
   }

   public void setIDCacheSize(final int idCacheSize)
   {
      this.idCacheSize = idCacheSize;
   }

   public boolean isPersistIDCache()
   {
      return persistIDCache;
   }

   public void setPersistIDCache(final boolean persist)
   {
      persistIDCache = persist;
   }

   public String getBindingsDirectory()
   {
      return bindingsDirectory;
   }

   public void setBindingsDirectory(final String dir)
   {
      bindingsDirectory = dir;
   }

   public String getJournalDirectory()
   {
      return journalDirectory;
   }

   public void setJournalDirectory(final String dir)
   {
      journalDirectory = dir;
   }

   public JournalType getJournalType()
   {
      return journalType;
   }

   public void setPagingDirectory(final String dir)
   {
      pagingDirectory = dir;
   }

   public String getPagingDirectory()
   {
      return pagingDirectory;
   }

   public void setJournalType(final JournalType type)
   {
      journalType = type;
   }

   public boolean isJournalSyncTransactional()
   {
      return journalSyncTransactional;
   }

   public void setJournalSyncTransactional(final boolean sync)
   {
      journalSyncTransactional = sync;
   }

   public boolean isJournalSyncNonTransactional()
   {
      return journalSyncNonTransactional;
   }

   public void setJournalSyncNonTransactional(final boolean sync)
   {
      journalSyncNonTransactional = sync;
   }

   public int getJournalFileSize()
   {
      return journalFileSize;
   }

   public void setJournalFileSize(final int size)
   {
      journalFileSize = size;
   }

   public int getJournalMaxAIO()
   {
      return journalMaxAIO;
   }

   public void setJournalMaxAIO(final int maxAIO)
   {
      journalMaxAIO = maxAIO;
   }

   public int getJournalMinFiles()
   {
      return journalMinFiles;
   }

   public void setJournalMinFiles(final int files)
   {
      journalMinFiles = files;
   }
      
   public boolean isLogJournalWriteRate()
   {
      return logJournalWriteRate;
   }

   public void setLogJournalWriteRate(boolean logJournalWriteRate)
   {
      this.logJournalWriteRate = logJournalWriteRate;
   }
     
   public int getJournalPerfBlastPages()
   {
      return journalPerfBlastPages;
   }

   public void setJournalPerfBlastPages(int journalPerfBlastPages)
   {
      this.journalPerfBlastPages = journalPerfBlastPages;
   }

   public boolean isCreateBindingsDir()
   {
      return createBindingsDir;
   }

   public void setCreateBindingsDir(final boolean create)
   {
      createBindingsDir = create;
   }

   public boolean isCreateJournalDir()
   {
      return createJournalDir;
   }

   public void setCreateJournalDir(final boolean create)
   {
      createJournalDir = create;
   }

   public boolean isWildcardRoutingEnabled()
   {
      return wildcardRoutingEnabled;
   }

   public void setWildcardRoutingEnabled(final boolean enabled)
   {
      wildcardRoutingEnabled = enabled;
   }

   public long getTransactionTimeout()
   {
      return transactionTimeout;
   }

   public void setTransactionTimeout(final long timeout)
   {
      transactionTimeout = timeout;
   }

   public long getTransactionTimeoutScanPeriod()
   {
      return transactionTimeoutScanPeriod;
   }

   public void setTransactionTimeoutScanPeriod(final long period)
   {
      transactionTimeoutScanPeriod = period;
   }

   public long getMessageExpiryScanPeriod()
   {
      return messageExpiryScanPeriod;
   }

   public void setMessageExpiryScanPeriod(final long messageExpiryScanPeriod)
   {
      this.messageExpiryScanPeriod = messageExpiryScanPeriod;
   }

   public int getMessageExpiryThreadPriority()
   {
      return messageExpiryThreadPriority;
   }

   public void setMessageExpiryThreadPriority(final int messageExpiryThreadPriority)
   {
      this.messageExpiryThreadPriority = messageExpiryThreadPriority;
   }

   public boolean isSecurityEnabled()
   {
      return securityEnabled;
   }

   public void setSecurityEnabled(final boolean enabled)
   {
      securityEnabled = enabled;
   }

   public boolean isJMXManagementEnabled()
   {
      return jmxManagementEnabled;
   }

   public void setJMXManagementEnabled(final boolean enabled)
   {
      jmxManagementEnabled = enabled;
   }


   public void setAIOBufferTimeout(int timeout)
   {
      this.journalAIOBufferTimeout = timeout;
   }
   
   public int getAIOBufferTimeout()
   {
      return journalAIOBufferTimeout;
   }

   public void setAIOFlushOnSync(boolean flush)
   {
      journalAIOFlushSync = flush;
   }

   public boolean isAIOFlushOnSync()
   {
      return journalAIOFlushSync;
   }

   public int getAIOBufferSize()
   {
      return journalAIOBufferSize;
   }

   public void setAIOBufferSize(int size)
   {
      this.journalAIOBufferSize = size;
   }
   
   public String getLargeMessagesDirectory()
   {
      return largeMessagesDirectory;
   }

   public void setLargeMessagesDirectory(final String directory)
   {
      largeMessagesDirectory = directory;
   }

   public boolean isMessageCounterEnabled()
   {
      return messageCounterEnabled;
   }

   public long getMessageCounterSamplePeriod()
   {
      return messageCounterSamplePeriod;
   }

   public int getMessageCounterMaxDayHistory()
   {
      return messageCounterMaxDayHistory;
   }

   public SimpleString getManagementAddress()
   {
      return managementAddress;
   }

   public void setManagementAddress(final SimpleString address)
   {
      managementAddress = address;
   }

   public SimpleString getManagementNotificationAddress()
   {
      return managementNotificationAddress;
   }

   public void setManagementNotificationAddress(final SimpleString address)
   {
      managementNotificationAddress = address;
   }

   public String getManagementClusterUser()
   {
      return managementClusterUser;
   }

   public String getManagementClusterPassword()
   {
      return managementClusterPassword;
   }

   public void setManagementClusterPassword(final String clusterPassword)
   {
      managementClusterPassword = clusterPassword;
   }

   public long getManagementRequestTimeout()
   {
      return managementRequestTimeout;
   }

   public void setManagementRequestTimeout(final long managementRequestTimeout)
   {
      this.managementRequestTimeout = managementRequestTimeout;
   }

   @Override
   public boolean equals(final Object other)
   {
      if (this == other)
      {
         return true;
      }

      if (other instanceof Configuration == false)
      {
         return false;
      }

      Configuration cother = (Configuration)other;

      return cother.isClustered() == isClustered() && cother.isCreateBindingsDir() == isCreateBindingsDir() &&
             cother.isCreateJournalDir() == isCreateJournalDir() &&
             cother.isJournalSyncNonTransactional() == isJournalSyncNonTransactional() &&
             cother.isJournalSyncTransactional() == isJournalSyncTransactional() &&
             cother.isSecurityEnabled() == isSecurityEnabled() &&
             cother.isWildcardRoutingEnabled() == isWildcardRoutingEnabled() &&
             cother.getLargeMessagesDirectory().equals(getLargeMessagesDirectory()) &&
             cother.getBindingsDirectory().equals(getBindingsDirectory()) &&
             cother.getJournalDirectory().equals(getJournalDirectory()) &&
             cother.getJournalFileSize() == getJournalFileSize() &&
             cother.getJournalMaxAIO() == getJournalMaxAIO() &&
             cother.getJournalMinFiles() == getJournalMinFiles() &&
             cother.getJournalType() == getJournalType() &&
             cother.getScheduledThreadPoolMaxSize() == getScheduledThreadPoolMaxSize() &&
             cother.getSecurityInvalidationInterval() == getSecurityInvalidationInterval() &&
             cother.getManagementAddress().equals(getManagementAddress());
   }

   public int getJournalCompactMinFiles()
   {
      return journalCompactMinFiles;
   }

   public int getJournalCompactPercentage()
   {
      return journalCompactPercentage;
   }
   
   public void setJournalCompactMinFiles(int minFiles)
   {
      this.journalCompactMinFiles = minFiles;
   }

   public void setJournalCompactPercentage(int percentage)
   {
      this.journalCompactPercentage = percentage;
   }
   
   public long getServerDumpInterval()
   {
      return serverDumpInterval;
   }
   
   public void getServerDumpInterval(long intervalInMilliseconds)
   {
      this.serverDumpInterval = intervalInMilliseconds;
   }
}
