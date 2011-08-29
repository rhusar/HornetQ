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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMember;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.BroadcastGroupConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.protocol.core.impl.wireformat.NodeAnnounceMessage;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.Transformer;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.Future;
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
public class ClusterManagerImpl implements ClusterManagerInternal
{
   private static final Logger log = Logger.getLogger(ClusterManagerImpl.class);

   private final Map<String, BroadcastGroup> broadcastGroups = new HashMap<String, BroadcastGroup>();

   private final Map<String, Bridge> bridges = new HashMap<String, Bridge>();

   private final ExecutorFactory executorFactory;

   private final HornetQServer server;

   private final PostOffice postOffice;

   private final ScheduledExecutorService scheduledExecutor;

   private final ManagementService managementService;

   private final Configuration configuration;

   private final UUID nodeUUID;

   private volatile boolean started;

   private volatile boolean backup;

   private final boolean clustered;

   // the cluster connections which links this node to other cluster nodes
   private final Map<String, ClusterConnection> clusterConnections = new HashMap<String, ClusterConnection>();

   private final Topology topology = new Topology(this);
   
   private TopologyMember localMember;

   private volatile ServerLocatorInternal backupServerLocator;

   private final Set<ServerLocatorInternal> clusterLocators = new ConcurrentHashSet<ServerLocatorInternal>();

   private final Executor executor;

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

      executor = executorFactory.getExecutor();;

      topology.setExecutor(executor);

      this.server = server;

      this.postOffice = postOffice;

      this.scheduledExecutor = scheduledExecutor;

      this.managementService = managementService;

      this.configuration = configuration;

      this.nodeUUID = nodeUUID;

      this.backup = backup;

      this.clustered = clustered;
   }

   public String describe()
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      out.println("Information on " + this);
      out.println("*******************************************************");
      out.println("Topology: " + topology.describe("Toopology on " + this));

      for (ClusterConnection conn : this.clusterConnections.values())
      {
         out.println(conn.describe());
      }

      out.println("*******************************************************");

      return str.toString();
   }

   public String toString()
   {
      return "ClusterManagerImpl[server=" + server + "]@" + System.identityHashCode(this);
   }
   
   public TopologyMember getLocalMember()
   {
      return localMember;
   }
   
   public String getNodeId()
   {
      return nodeUUID.toString();
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

         String connectorName = null;

         for (ClusterConnectionConfiguration config : configuration.getClusterConfigurations())
         {
            if (connectorName == null)
            {
               connectorName = config.getConnectorName();
            }
            else if (!connectorName.equals(config.getConnectorName()))
            {
               throw new IllegalStateException("Using multiple connector names on cluster connections it's "
                                               + "not supported at this time");
            }

         }

         if (connectorName != null)
         {
            TransportConfiguration nodeConnector = configuration.getConnectorConfigurations().get(connectorName);
            if (nodeConnector == null)
            {
               log.warn("No connecor with name '" + connectorName +
                        "'. The cluster connection will not be deployed.");
               return;
            }
   
            // Now announce presence
            announceNode(nodeConnector);
   
            for (ClusterConnectionConfiguration config : configuration.getClusterConfigurations())
            {
               deployClusterConnection(config);
            }
         }

      }

      for (BridgeConfiguration config : configuration.getBridgeConfigurations())
      {
         deployBridge(config);
      }

      started = true;
   }

   public void stop() throws Exception
   {
      synchronized (this)
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

            broadcastGroups.clear();

            for (ClusterConnection clusterConnection : clusterConnections.values())
            {
               clusterConnection.stop();
               managementService.unregisterCluster(clusterConnection.getName().toString());
            }

         }

         for (Bridge bridge : bridges.values())
         {
            bridge.stop();
            managementService.unregisterBridge(bridge.getName().toString());
         }

         bridges.clear();

         if (backupServerLocator != null)
         {
            backupServerLocator.close();
            backupServerLocator = null;
         }
      }

      for (ServerLocatorInternal clusterLocator : clusterLocators)
      {
         try
         {
            clusterLocator.close();
         }
         catch (Exception e)
         {
            log.warn("Error closing serverLocator=" + clusterLocator + ", message=" + e.getMessage(), e);
         }
      }
      clusterLocators.clear();
      started = false;

      clusterConnections.clear();
   }

   public void nodeAnnounced(final long uniqueEventID,
                             final String nodeID,
                             final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                             final boolean backup)
   {
      if (log.isDebugEnabled())
      {
         log.debug(this + "::NodeAnnounced, backup=" + backup + nodeID + connectorPair);
      }

      TopologyMember newMember = new TopologyMember(connectorPair.a, connectorPair.b);
      newMember.setUniqueEventID(uniqueEventID);
      if (backup)
      {
         topology.updateBackup(nodeID, new TopologyMember(connectorPair.a, connectorPair.b));
      }
      else
      {
         topology.updateMember(uniqueEventID, nodeID, newMember);
      }
   }

   public void flushExecutor()
   {
      Future future = new Future();
      executor.execute(future);
      if (!future.await(10000))
      {
         server.threadDump("Couldn't flush ClusterManager executor (" + this +
                           ") in 10 seconds, verify your thread pool size");
      }
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

   public void addClusterTopologyListener(final ClusterTopologyListener listener, final boolean clusterConnection)
   {
      topology.addClusterTopologyListener(listener);

      // no need to use an executor here since the Topology is already using one
      topology.sendTopology(listener);
   }

   public void removeClusterTopologyListener(final ClusterTopologyListener listener, final boolean clusterConnection)
   {
      topology.removeClusterTopologyListener(listener);
   }

   public Topology getTopology()
   {
      return topology;
   }

   // backup node becomes live
   public synchronized void activate()
   {
      if (backup)
      {
         backup = false;

         String nodeID = server.getNodeID().toString();

         TopologyMember member = topology.getMember(nodeID);
         // swap backup as live and send it to everybody
         member = new TopologyMember(member.getConnector().b, null);
         topology.updateAsLive(nodeID, member);

         if (backupServerLocator != null)
         {
            // todo we could use the topology of this to preempt it arriving from the cc
            try
            {
               backupServerLocator.close();
            }
            catch (Exception e)
            {
               log.warn("problem closing backup session factory", e);
            }
            backupServerLocator = null;
         }

         for (BroadcastGroup broadcastGroup : broadcastGroups.values())
         {
            try
            {
               broadcastGroup.start();
               broadcastGroup.activate();
            }
            catch (Exception e)
            {
               log.warn("unable to start broadcast group " + broadcastGroup.getName(), e);
            }
         }

         for (ClusterConnection clusterConnection : clusterConnections.values())
         {
            try
            {
               clusterConnection.activate();
            }
            catch (Exception e)
            {
               log.warn("unable to start cluster connection " + clusterConnection.getName(), e);
            }
         }

         for (Bridge bridge : bridges.values())
         {
            try
            {
               bridge.start();
            }
            catch (Exception e)
            {
               log.warn("unable to start bridge " + bridge.getName(), e);
            }
         }

         topology.sendMember(nodeID);
      }
   }

   public void announceBackup() throws Exception
   {
      List<ClusterConnectionConfiguration> configs = this.configuration.getClusterConfigurations();
      if (!configs.isEmpty())
      {
         ClusterConnectionConfiguration config = configs.get(0);

         TransportConfiguration connector = configuration.getConnectorConfigurations().get(config.getConnectorName());

         if (connector == null)
         {
            log.warn("No connecor with name '" + config.getConnectorName() + "'. backup cannot be announced.");
            return;
         }
         announceBackup(config, connector);
      }
      else
      {
         log.warn("no cluster connections defined, unable to announce backup");
      }
   }

   public void addClusterLocator(final ServerLocatorInternal serverLocator)
   {
      this.clusterLocators.add(serverLocator);
   }

   public void removeClusterLocator(final ServerLocatorInternal serverLocator)
   {
      this.clusterLocators.remove(serverLocator);
   }

   private synchronized void announceNode(final TransportConfiguration nodeConnector)
   {
      String nodeID = server.getNodeID().toString();
      
      if (backup)
      {
         localMember = new TopologyMember(null, nodeConnector);
      }
      else
      {
         localMember = new TopologyMember(nodeConnector, null);
      }

      System.out.println("Adding local member " + localMember);
      topology.updateAsLive(nodeID, localMember);
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

      if (!backup)
      {
         group.start();
      }
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

   public synchronized void deployBridge(final BridgeConfiguration config) throws Exception
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

      ServerLocatorInternal serverLocator;

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
            serverLocator = (ServerLocatorInternal)HornetQClient.createServerLocatorWithHA(discoveryGroupConfiguration);
         }
         else
         {
            serverLocator = (ServerLocatorInternal)HornetQClient.createServerLocatorWithoutHA(discoveryGroupConfiguration);
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
            serverLocator = (ServerLocatorInternal)HornetQClient.createServerLocatorWithHA(tcConfigs);
         }
         else
         {
            serverLocator = (ServerLocatorInternal)HornetQClient.createServerLocatorWithoutHA(tcConfigs);
         }

      }

      serverLocator.setConfirmationWindowSize(config.getConfirmationWindowSize());

      // We are going to manually retry on the bridge in case of failure
      serverLocator.setReconnectAttempts(0);
      serverLocator.setInitialConnectAttempts(-1);
      serverLocator.setRetryInterval(config.getRetryInterval());
      serverLocator.setMaxRetryInterval(config.getMaxRetryInterval());
      serverLocator.setRetryIntervalMultiplier(config.getRetryIntervalMultiplier());
      serverLocator.setClientFailureCheckPeriod(config.getClientFailureCheckPeriod());
      serverLocator.setBlockOnDurableSend(!config.isUseDuplicateDetection());
      serverLocator.setBlockOnNonDurableSend(!config.isUseDuplicateDetection());
      if (!config.isUseDuplicateDetection())
      {
         log.debug("Bridge " + config.getName() +
                   " is configured to not use duplicate detecion, it will send messages synchronously");
      }

      clusterLocators.add(serverLocator);

      Bridge bridge = new BridgeImpl(serverLocator,
                                     config.getReconnectAttempts(),
                                     config.getRetryInterval(),
                                     config.getRetryIntervalMultiplier(),
                                     config.getMaxRetryInterval(),
                                     nodeUUID,
                                     new SimpleString(config.getName()),
                                     queue,
                                     executorFactory.getExecutor(),
                                     SimpleString.toSimpleString(config.getFilterString()),
                                     SimpleString.toSimpleString(config.getForwardingAddress()),
                                     scheduledExecutor,
                                     transformer,
                                     config.isUseDuplicateDetection(),
                                     config.getUser(),
                                     config.getPassword(),
                                     !backup,
                                     server.getStorageManager());

      bridges.put(config.getName(), bridge);

      managementService.registerBridge(bridge, config);

      if (!backup)
      {
         bridge.start();
      }
   }

   public void destroyBridge(final String name) throws Exception
   {
      Bridge bridge;

      synchronized (this)
      {
         bridge = bridges.remove(name);
         if (bridge != null)
         {
            bridge.stop();
            managementService.unregisterBridge(name);
         }
      }

      bridge.flushExecutor();
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

      TransportConfiguration connector = configuration.getConnectorConfigurations().get(config.getConnectorName());

      if (connector == null)
      {
         log.warn("No connecor with name '" + config.getConnectorName() +
                  "'. The cluster connection will not be deployed.");
         return;
      }

      if (clusterConnections.containsKey(config.getName()))
      {
         log.warn("Cluster Configuration  '" + config.getConnectorName() +
                  "' already exists. The cluster connection will not be deployed.", new Exception("trace"));
         return;
      }

      ClusterConnectionImpl clusterConnection;

      if (config.getDiscoveryGroupName() != null)
      {
         DiscoveryGroupConfiguration dg = configuration.getDiscoveryGroupConfigurations()
                                                       .get(config.getDiscoveryGroupName());

         if (dg == null)
         {
            ClusterManagerImpl.log.warn("No discovery group with name '" + config.getDiscoveryGroupName() +
                                        "'. The cluster connection will not be deployed.");
            return;
         }

         if (log.isDebugEnabled())
         {
            log.debug(this + " Starting a Discovery Group Cluster Connection, name=" +
                      config.getDiscoveryGroupName() +
                      ", dg=" +
                      dg);
         }

         clusterConnection = new ClusterConnectionImpl(this,
                                                       topology,
                                                       dg,
                                                       connector,
                                                       new SimpleString(config.getName()),
                                                       new SimpleString(config.getAddress()),
                                                       config.getClientFailureCheckPeriod(),
                                                       config.getConnectionTTL(),
                                                       config.getRetryInterval(),
                                                       config.getRetryIntervalMultiplier(),
                                                       config.getMaxRetryInterval(),
                                                       config.getReconnectAttempts(),
                                                       config.getCallTimeout(),
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
                                                       server.getConfiguration().getClusterPassword(),
                                                       config.isAllowDirectConnectionsOnly());
      }
      else
      {
         TransportConfiguration[] tcConfigs = config.getStaticConnectors() != null ? connectorNameListToArray(config.getStaticConnectors())
                                                                                  : null;

         if (log.isDebugEnabled())
         {
            log.debug(this + " defining cluster connection towards " + Arrays.toString(tcConfigs));
         }

         clusterConnection = new ClusterConnectionImpl(this,
                                                       topology,
                                                       tcConfigs,
                                                       connector,
                                                       new SimpleString(config.getName()),
                                                       new SimpleString(config.getAddress()),
                                                       config.getClientFailureCheckPeriod(),
                                                       config.getConnectionTTL(),
                                                       config.getRetryInterval(),
                                                       config.getRetryIntervalMultiplier(),
                                                       config.getMaxRetryInterval(),
                                                       config.getReconnectAttempts(),
                                                       config.getCallTimeout(),
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
                                                       server.getConfiguration().getClusterPassword(),
                                                       config.isAllowDirectConnectionsOnly());
      }

      managementService.registerCluster(clusterConnection, config);

      clusterConnections.put(config.getName(), clusterConnection);

      if (log.isDebugEnabled())
      {
         log.debug("ClusterConnection.start at " + clusterConnection, new Exception("trace"));
      }
      clusterConnection.start();

      if (backup)
      {
         announceBackup(config, connector);
      }
   }

   private void announceBackup(final ClusterConnectionConfiguration config, final TransportConfiguration connector) throws Exception
   {
      if (config.getStaticConnectors() != null)
      {
         TransportConfiguration[] tcConfigs = connectorNameListToArray(config.getStaticConnectors());

         backupServerLocator = (ServerLocatorInternal)HornetQClient.createServerLocatorWithoutHA(tcConfigs);
         backupServerLocator.setReconnectAttempts(-1);
         backupServerLocator.setInitialConnectAttempts(-1);
      }
      else if (config.getDiscoveryGroupName() != null)
      {
         DiscoveryGroupConfiguration dg = configuration.getDiscoveryGroupConfigurations()
                                                       .get(config.getDiscoveryGroupName());

         if (dg == null)
         {
            ClusterManagerImpl.log.warn("No discovery group with name '" + config.getDiscoveryGroupName() +
                                        "'. The cluster connection will not be deployed.");
         }

         backupServerLocator = (ServerLocatorInternal)HornetQClient.createServerLocatorWithoutHA(dg);
         backupServerLocator.setReconnectAttempts(-1);
         backupServerLocator.setInitialConnectAttempts(-1);
      }
      else
      {
         return;
      }
      log.info("announcing backup");
      executor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               if (log.isDebugEnabled())
               {
                  log.debug(ClusterManagerImpl.this + ":: announcing " + connector + " to " + backupServerLocator);
               }
               ClientSessionFactory backupSessionFactory = backupServerLocator.connect();
               if (backupSessionFactory != null)
               {
                  System.out.println("announcing " + System.currentTimeMillis());
                  backupSessionFactory.getConnection()
                                      .getChannel(0, -1)
                                      .send(new NodeAnnounceMessage(System.currentTimeMillis(),
                                                                    nodeUUID.toString(),
                                                                    true,
                                                                    connector,
                                                                    null));
                  log.info("backup announced");
               }
            }
            catch (Exception e)
            {
               log.warn("Unable to announce backup, retrying", e);
            }
         }
      });
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

   // for testing
   public void clear()
   {
      for (Bridge bridge : bridges.values())
      {
         try
         {
            bridge.stop();
         }
         catch (Exception e)
         {
            log.warn(e.getMessage(), e);
         }
      }
      bridges.clear();
      for (ClusterConnection clusterConnection : clusterConnections.values())
      {
         try
         {
            clusterConnection.stop();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
      clusterConnections.clear();
   }
}
