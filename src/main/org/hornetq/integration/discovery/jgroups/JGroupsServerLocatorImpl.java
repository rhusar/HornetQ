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

package org.hornetq.integration.discovery.jgroups;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.client.impl.AbstractServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.ConfigurationHelper;

/**
 * A JGroupsServerLocatorImpl
 *
 * @author "<a href=\"tm.igarashi@gmail.com\">Tomohisa Igarashi</a>"
 *
 *
 */
public class JGroupsServerLocatorImpl extends AbstractServerLocator implements DiscoveryListener
{
   private static final long serialVersionUID = 1720602999991968346L;

   private static final Logger log = Logger.getLogger(JGroupsServerLocatorImpl.class);

   private String discoveryGroupName;
   
   private String jgroupsConfigurationFileName;

   private String jgroupsChannelName;
   
   private long initialWaitTimeout;
   
   private long refreshTimeout;
   
   private DiscoveryGroup discoveryGroup;
   
   private volatile boolean closing;
   
   private synchronized void initialise() throws Exception {
      if (!isReadOnly())
      {
         setThreadPools();

         instantiateLoadBalancingPolicy();

         this.discoveryGroupName = getDiscoveryGroupConfiguration().getName();
         
         Map<String,Object> params = getDiscoveryGroupConfiguration().getParams();

         this.jgroupsChannelName = ConfigurationHelper.getStringProperty(DiscoveryGroupConstants.JGROUPS_CHANNEL_NAME_NAME, DiscoveryGroupConstants.DEFAULT_JGROUPS_CHANNEL_NAME, params);
         this.initialWaitTimeout = ConfigurationHelper.getLongProperty(DiscoveryGroupConstants.INITIAL_WAIT_TIMEOUT_NAME, HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT, params);

         this.refreshTimeout = ConfigurationHelper.getLongProperty(DiscoveryGroupConstants.REFRESH_TIMEOUT_NAME, ConfigurationImpl.DEFAULT_BROADCAST_REFRESH_TIMEOUT, params);

         this.jgroupsConfigurationFileName = ConfigurationHelper.getStringProperty(DiscoveryGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME, null, params);
         
         this.discoveryGroup = new JGroupsDiscoveryGroupImpl(getNodeID(),
                                                             this.discoveryGroupName,
                                                             this.jgroupsChannelName,
                                                             Thread.currentThread().getContextClassLoader().getResource(this.jgroupsConfigurationFileName),
                                                             this.refreshTimeout);
         
         this.discoveryGroup.registerListener(this);
         
         this.discoveryGroup.start();

         setReadOnly(true);
      }
   }
   
   public JGroupsServerLocatorImpl(boolean useHA, DiscoveryGroupConfiguration discoveryGroupConfiguration)
   {
      super(useHA, discoveryGroupConfiguration);
   }

    public void start(Executor executor) throws Exception
   {
       initialise();

       executor.execute(new Runnable()
       {
          public void run()
          {
             try
             {
                connect();
             }
             catch (Exception e)
             {
                if (!closing)
                {
                   log.warn("did not connect the cluster connection to other nodes", e);
                }
             }
          }
       });
   }

   public ClientSessionFactory connect() throws Exception
   {
      ClientSessionFactoryInternal sf;

      // wait for discovery group to get the list of initial connectors
      sf = (ClientSessionFactoryInternal)createSessionFactory();

      addFactory(sf);
      return sf;
   }

   public ClientSessionFactory createSessionFactory() throws Exception
   {
      if (isClosed())
      {
         throw new IllegalStateException("Cannot create session factory, server locator is closed (maybe it has been garbage collected)");
      }

      try
      {
         initialise();
      }
      catch (Exception e)
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR, "Failed to initialise session factory", e);
      }

      if (getInitialConnectors() == null)
      {
         // Wait for an initial broadcast to give us at least one node in the cluster
         long timeout = isClusterConnection() ? 0 : this.initialWaitTimeout;
         boolean ok = discoveryGroup.waitForBroadcast(timeout);

         if (!ok)
         {
            throw new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                                       "Timed out waiting to receive initial broadcast from cluster");
         }
      }

      ClientSessionFactoryInternal factory = null;

      synchronized (this)
      {
         boolean retry;
         int attempts = 0;
         do
         {
            retry = false;

            TransportConfiguration tc = selectConnector();

            // try each factory in the list until we find one which works

            try
            {
               factory = new ClientSessionFactoryImpl(this,
                                                      tc,
                                                      getCallTimeout(),
                                                      getClientFailureCheckPeriod(),
                                                      getConnectionTTL(),
                                                      getRetryInterval(),
                                                      getRetryIntervalMultiplier(),
                                                      getMaxRetryInterval(),
                                                      getReconnectAttempts(),
                                                      getThreadPool(),
                                                      getScheduledThreadPool(),
                                                      getInterceptors());
               factory.connect(getInitialConnectAttempts(), isFailoverOnInitialConnection());
            }
            catch (HornetQException e)
            {
               factory.close();
               factory = null;
               if (e.getCode() == HornetQException.NOT_CONNECTED)
               {
                  attempts++;

                  if (attempts == getConnectorLength())
                  {
                     throw new HornetQException(HornetQException.NOT_CONNECTED,
                                                "Cannot connect to server(s). Tried with all available servers.");
                  }
                  retry = true;
               }
               else
               {
                  throw e;
               }
            }
         }
         while (retry);

         if (isHA())
         {
            long toWait = 30000;
            long start = System.currentTimeMillis();
            while (!isReceivedTopology() && toWait > 0)
            {
               // Now wait for the topology

               try
               {
                  wait(toWait);
               }
               catch (InterruptedException ignore)
               {
               }

               long now = System.currentTimeMillis();

               toWait -= now - start;

               start = now;
            }

            if (toWait <= 0)
            {
               throw new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                                          "Timed out waiting to receive cluster topology");
            }
         }

         addFactory(factory);

         return factory;
      }
   }

   public ClientSessionFactory createSessionFactory(TransportConfiguration transportConfiguration) throws Exception
   {
      if (isClosed())
      {
         throw new IllegalStateException("Cannot create session factory, server locator is closed (maybe it has been garbage collected)");
      }

      try
      {
         initialise();
      }
      catch (Exception e)
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR, "Failed to initialise session factory", e);
      }

      ClientSessionFactoryInternal factory = new ClientSessionFactoryImpl(this,
                                                                          transportConfiguration,
                                                                          getCallTimeout(),
                                                                          getClientFailureCheckPeriod(),
                                                                          getConnectionTTL(),
                                                                          getRetryInterval(),
                                                                          getRetryIntervalMultiplier(),
                                                                          getMaxRetryInterval(),
                                                                          getReconnectAttempts(),
                                                                          getThreadPool(),
                                                                          getScheduledThreadPool(),
                                                                          getInterceptors());

      factory.connect(getReconnectAttempts(), isFailoverOnInitialConnection());

      addFactory(factory);

      return factory;
   }

   public void close()
   {
      if (isClosed())
      {
         return;
      }

      closing = true;

      try
      {
         this.discoveryGroup.stop();
      }
      catch (Exception e)
      {
         log.error("Failed to stop discovery group", e);
      }

      super.close();
   }
   
   public synchronized void connectorsChanged()
   {
      List<DiscoveryEntry> newConnectors = discoveryGroup.getDiscoveryEntries();

      TransportConfiguration[] initialConnectors = (TransportConfiguration[])Array.newInstance(TransportConfiguration.class,
                                                                                               newConnectors.size());
      int count = 0;
      for (DiscoveryEntry entry : newConnectors)
      {
         initialConnectors[count++] = entry.getConnector();
      }

      if (isHA() && isClusterConnection() && !isReceivedTopology() && initialConnectors.length > 0)
      {
         // FIXME the node is alone in the cluster. We create a connection to the new node
         // to trigger the node notification to form the cluster.
         try
         {
            connect();
         }
         catch (Exception e)
         {
            e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
         }
      }

      setInitialConnectors(initialConnectors);
   }
}
