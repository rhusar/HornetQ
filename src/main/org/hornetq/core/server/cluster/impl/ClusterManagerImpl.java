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

package org.hornetq.core.server.cluster.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.lang.reflect.Array;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.BroadcastGroupConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.DiscoveryGroupConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.cluster.Transformer;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.UUID;

/**
 * A ClusterManagerImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Nov 2008 09:23:49
 *
 *
 */
public class ClusterManagerImpl implements ClusterManager
{
   private static final Logger log = Logger.getLogger(ClusterManagerImpl.class);

   private final Map<String, BroadcastGroup> broadcastGroups = new HashMap<String, BroadcastGroup>();

   private final Map<String, Bridge> bridges = new HashMap<String, Bridge>();

   private final Map<String, ClusterConnection> clusterConnections = new HashMap<String, ClusterConnection>();

   private final ExecutorFactory executorFactory;

   private final HornetQServer server;

   private final PostOffice postOffice;

   private final ScheduledExecutorService scheduledExecutor;

   private final ManagementService managementService;

   private final Configuration configuration;

   private final UUID nodeUUID;

   private volatile boolean started;

   private boolean backup;

   private final boolean clustered;

   public ClusterManagerImpl(final ExecutorFactory executorFactory,
                             final HornetQServer server,
                             final PostOffice postOffice,
                             final ScheduledExecutorService scheduledExecutor,
                             final ManagementService managementService,
                             final Configuration configuration,
                             final UUID nodeUUID,
                             final boolean backup,
                             final boolean clustered)
   {
      if (nodeUUID == null)
      {
         throw new IllegalArgumentException("Node uuid is null");
      }

      this.executorFactory = executorFactory;

      this.server = server;

      this.postOffice = postOffice;

      this.scheduledExecutor = scheduledExecutor;

      this.managementService = managementService;

      this.configuration = configuration;

      this.nodeUUID = nodeUUID;

      this.backup = backup;

      this.clustered = clustered;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      if (clustered)
      {
         for (BroadcastGroupConfiguration config : configuration.getBroadcastGroupConfigurations())
         {
            deployBroadcastGroup(config);
         }

         for (ClusterConnectionConfiguration config : configuration.getClusterConfigurations())
         {
            deployClusterConnection(config);
         }

      }

      for (BridgeConfiguration config : configuration.getBridgeConfigurations())
      {
         deployBridge(config);
      }

      started = true;
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      if (clustered)
      {
         for (BroadcastGroup group : broadcastGroups.values())
         {
            group.stop();
            managementService.unregisterBroadcastGroup(group.getName());
         }

         for (ClusterConnection clusterConnection : clusterConnections.values())
         {
            clusterConnection.stop();
            managementService.unregisterCluster(clusterConnection.getName().toString());
         }

         broadcastGroups.clear();
      }

      for (Bridge bridge : bridges.values())
      {
         bridge.stop();
         managementService.unregisterBridge(bridge.getName().toString());
      }

      bridges.clear();

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   public Map<String, Bridge> getBridges()
   {
      return new HashMap<String, Bridge>(bridges);
   }

   public Set<ClusterConnection> getClusterConnections()
   {
      return new HashSet<ClusterConnection>(clusterConnections.values());
   }

   public Set<BroadcastGroup> getBroadcastGroups()
   {
      return new HashSet<BroadcastGroup>(broadcastGroups.values());
   }

   public ClusterConnection getClusterConnection(final SimpleString name)
   {
      return clusterConnections.get(name.toString());
   }

   public synchronized void activate()
   {
      for (BroadcastGroup bg : broadcastGroups.values())
      {
         bg.activate();
      }

      for (Bridge bridge : bridges.values())
      {
         bridge.activate();
      }

      for (ClusterConnection cc : clusterConnections.values())
      {
         cc.activate();
      }

      backup = false;
   }

   public void startAnnouncement()
   {

   }

   public void stopAnnouncement()
   {

   }

   private Set<ClusterTopologyListener> clientListeners = new ConcurrentHashSet<ClusterTopologyListener>();

   private Set<ClusterTopologyListener> clusterConnectionListeners = new ConcurrentHashSet<ClusterTopologyListener>();

   private Map<String, Pair<TransportConfiguration, TransportConfiguration>> topology;

   public synchronized void registerTopologyListener(final ClusterTopologyListener listener,
                                                     final boolean clusterConnection)
   {
      if (clusterConnection)
      {
         this.clusterConnectionListeners.add(listener);
      }
      else
      {
         this.clientListeners.add(listener);
      }

      // We now need to send the current topology to the client

      int count = 0;
      for (Map.Entry<String, Pair<TransportConfiguration, TransportConfiguration>> entry : topology.entrySet())
      {
         listener.nodeUP(entry.getKey(), entry.getValue(), ++count == topology.size());
      }
   }

   public synchronized void unregisterTopologyListener(final ClusterTopologyListener listener,
                                                       final boolean clusterConnection)
   {
      if (clusterConnection)
      {
         this.clusterConnectionListeners.remove(listener);
      }
      else
      {
         this.clientListeners.remove(listener);
      }
   }

   public synchronized void announceNode(final String nodeID,
                                         final boolean backup,
                                         final TransportConfiguration connector)
   {
      Pair<TransportConfiguration, TransportConfiguration> pair = topology.get(nodeID);

      if (pair == null)
      {
         if (backup)
         {
            pair = new Pair<TransportConfiguration, TransportConfiguration>(null, connector);
         }
         else
         {
            pair = new Pair<TransportConfiguration, TransportConfiguration>(connector, null);
         }

         topology.put(nodeID, pair);
      }
      else
      {
         if (backup)
         {
            pair.b = connector;
         }
         else
         {
            pair.a = connector;
         }
      }

      // Propagate the announcement

      for (ClusterTopologyListener listener : clientListeners)
      {
         listener.nodeUP(nodeID, pair, false);
      }

      for (ClusterTopologyListener listener : clusterConnectionListeners)
      {
         listener.nodeUP(nodeID, pair, false);
      }

   }

   public synchronized void nodeDown(final String nodeID)
   {
      topology.remove(nodeID);

      for (ClusterTopologyListener listener : clientListeners)
      {
         listener.nodeDown(nodeID);
      }
   }

   public synchronized void nodeUP(final String nodeID,
                                   final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                                   final boolean last)
   {
      topology.put(nodeID, connectorPair);

      for (ClusterTopologyListener listener : clientListeners)
      {
         listener.nodeUP(nodeID, connectorPair, false);
      }
   }

   private synchronized void deployBroadcastGroup(final BroadcastGroupConfiguration config) throws Exception
   {
      if (broadcastGroups.containsKey(config.getName()))
      {
         ClusterManagerImpl.log.warn("There is already a broadcast-group with name " + config.getName() +
                                     " deployed. This one will not be deployed.");

         return;
      }

      InetAddress localAddress = null;
      if (config.getLocalBindAddress() != null)
      {
         localAddress = InetAddress.getByName(config.getLocalBindAddress());
      }

      InetAddress groupAddress = InetAddress.getByName(config.getGroupAddress());

      BroadcastGroupImpl group = new BroadcastGroupImpl(nodeUUID.toString(),
                                                        config.getName(),
                                                        localAddress,
                                                        config.getLocalBindPort(),
                                                        groupAddress,
                                                        config.getGroupPort(),
                                                        !backup);

      for (String connectorInfo : config.getConnectorInfos())
      {
         TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorInfo);

         if (connector == null)
         {
            logWarnNoConnector(config.getName(), connectorInfo);

            return;
         }

         group.addConnector(connector);
      }

      ScheduledFuture<?> future = scheduledExecutor.scheduleWithFixedDelay(group,
                                                                           0L,
                                                                           config.getBroadcastPeriod(),
                                                                           MILLISECONDS);

      group.setScheduledFuture(future);

      broadcastGroups.put(config.getName(), group);

      managementService.registerBroadcastGroup(group, config);

      group.start();
   }

   private void logWarnNoConnector(final String connectorName, final String bgName)
   {
      ClusterManagerImpl.log.warn("There is no connector deployed with name '" + connectorName +
                                  "'. The broadcast group with name '" +
                                  bgName +
                                  "' will not be deployed.");
   }

   private TransportConfiguration[] connectorNameListToArray(final List<String> connectorNames)
   {
      TransportConfiguration[] tcConfigs = (TransportConfiguration[])Array.newInstance(TransportConfiguration.class,
                                                                                       connectorNames.size());
      int count = 0;
      for (String connectorName : connectorNames)
      {
         TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorName);

         if (connector == null)
         {
            ClusterManagerImpl.log.warn("No connector defined with name '" + connectorName +
                                        "'. The bridge will not be deployed.");

            return null;
         }

         tcConfigs[count++] = connector;
      }

      return tcConfigs;
   }

   private synchronized void deployBridge(final BridgeConfiguration config) throws Exception
   {
      if (config.getName() == null)
      {
         ClusterManagerImpl.log.warn("Must specify a unique name for each bridge. This one will not be deployed.");

         return;
      }

      if (config.getQueueName() == null)
      {
         ClusterManagerImpl.log.warn("Must specify a queue name for each bridge. This one will not be deployed.");

         return;
      }

      if (config.getForwardingAddress() == null)
      {
         ClusterManagerImpl.log.debug("Forward address is not specified. Will use original message address instead");
      }

      if (bridges.containsKey(config.getName()))
      {
         ClusterManagerImpl.log.warn("There is already a bridge with name " + config.getName() +
                                     " deployed. This one will not be deployed.");

         return;
      }

      Transformer transformer = instantiateTransformer(config.getTransformerClassName());

      Binding binding = postOffice.getBinding(new SimpleString(config.getQueueName()));

      if (binding == null)
      {
         ClusterManagerImpl.log.warn("No queue found with name " + config.getQueueName() +
                                     " bridge will not be deployed.");

         return;
      }

      Queue queue = (Queue)binding.getBindable();

      ServerLocator serverLocator;

      if (config.getDiscoveryGroupName() != null)
      {
         DiscoveryGroupConfiguration discoveryGroupConfiguration = configuration.getDiscoveryGroupConfigurations()
                                                                                .get(config.getDiscoveryGroupName());
         if (discoveryGroupConfiguration == null)
         {
            ClusterManagerImpl.log.warn("No discovery group configured with name '" + config.getDiscoveryGroupName() +
                                        "'. The bridge will not be deployed.");

            return;
         }

         if (config.isHA())
         {
            serverLocator = HornetQClient.createServerLocatorWithHA(discoveryGroupConfiguration.getGroupAddress(),
                                                                    discoveryGroupConfiguration.getGroupPort());
         }
         else
         {
            serverLocator = HornetQClient.createServerLocatorWithoutHA(discoveryGroupConfiguration.getGroupAddress(),
                                                                       discoveryGroupConfiguration.getGroupPort());
         }

      }
      else
      {
         TransportConfiguration[] tcConfigs = connectorNameListToArray(config.getStaticConnectors());

         if (tcConfigs == null)
         {
            return;
         }

         if (config.isHA())
         {
            serverLocator = HornetQClient.createServerLocatorWithHA(tcConfigs);
         }
         else
         {
            serverLocator = HornetQClient.createServerLocatorWithoutHA(tcConfigs);
         }

      }

      serverLocator.setConfirmationWindowSize(config.getConfirmationWindowSize());
      serverLocator.setFailoverOnServerShutdown(config.isFailoverOnServerShutdown());
      serverLocator.setReconnectAttempts(config.getReconnectAttempts());
      serverLocator.setRetryInterval(config.getRetryInterval());
      serverLocator.setRetryIntervalMultiplier(config.getRetryIntervalMultiplier());
      serverLocator.setClientFailureCheckPeriod(config.getClientFailureCheckPeriod());

      Bridge bridge = new BridgeImpl(serverLocator,
                                     nodeUUID,
                                     new SimpleString(config.getName()),
                                     queue,
                                     executorFactory.getExecutor(),
                                     SimpleString.toSimpleString(config.getFilterString()),
                                     new SimpleString(config.getForwardingAddress()),
                                     scheduledExecutor,
                                     transformer,
                                     config.isUseDuplicateDetection(),
                                     config.getUser(),
                                     config.getPassword(),
                                     !backup,
                                     server.getStorageManager());

      bridges.put(config.getName(), bridge);

      managementService.registerBridge(bridge, config);

      bridge.start();
   }

   private synchronized void deployClusterConnection(final ClusterConnectionConfiguration config) throws Exception
   {
      if (config.getName() == null)
      {
         ClusterManagerImpl.log.warn("Must specify a unique name for each cluster. This one will not be deployed.");

         return;
      }

      if (config.getAddress() == null)
      {
         ClusterManagerImpl.log.warn("Must specify an address for each cluster connection. This one will not be deployed.");

         return;
      }

      ServerLocator serverLocator;

      if (config.getStaticConnectors() != null)
      {
         TransportConfiguration[] tcConfigs = connectorNameListToArray(config.getStaticConnectors());

         serverLocator = HornetQClient.createServerLocatorWithHA(tcConfigs);
      }
      else
      {
         DiscoveryGroupConfiguration dg = configuration.getDiscoveryGroupConfigurations()
                                                       .get(config.getDiscoveryGroupName());

         if (dg == null)
         {
            ClusterManagerImpl.log.warn("No discovery group with name '" + config.getDiscoveryGroupName() +
                                        "'. The cluster connection will not be deployed.");
         }

         serverLocator = HornetQClient.createServerLocatorWithHA(dg.getGroupAddress(), dg.getGroupPort());
      }

      ClusterConnection clusterConnection = new ClusterConnectionImpl(serverLocator,
                                                                      new SimpleString(config.getName()),
                                                                      new SimpleString(config.getAddress()),
                                                                      config.getRetryInterval(),
                                                                      config.isDuplicateDetection(),
                                                                      config.isForwardWhenNoConsumers(),
                                                                      config.getConfirmationWindowSize(),
                                                                      executorFactory,
                                                                      server,
                                                                      postOffice,
                                                                      managementService,
                                                                      scheduledExecutor,
                                                                      config.getMaxHops(),
                                                                      nodeUUID,
                                                                      backup,
                                                                      server.getConfiguration().getClusterUser(),
                                                                      server.getConfiguration().getClusterPassword());

      managementService.registerCluster(clusterConnection, config);

      clusterConnections.put(config.getName(), clusterConnection);

      clusterConnection.start();
   }

   private Transformer instantiateTransformer(final String transformerClassName)
   {
      Transformer transformer = null;

      if (transformerClassName != null)
      {
         ClassLoader loader = Thread.currentThread().getContextClassLoader();
         try
         {
            Class<?> clz = loader.loadClass(transformerClassName);
            transformer = (Transformer)clz.newInstance();
         }
         catch (Exception e)
         {
            throw new IllegalArgumentException("Error instantiating transformer class \"" + transformerClassName + "\"",
                                               e);
         }
      }
      return transformer;
   }

}
