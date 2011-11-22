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

package org.hornetq.core.client.impl;

import java.net.InetAddress;
import java.util.Map;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConstants;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.impl.DiscoveryGroupImpl;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.ConfigurationHelper;

/**
 * A UDPServerLocatorImpl
 * @author Tim Fox
 */
public class UDPServerLocatorImpl extends AbstractServerLocator
{
   private static final long serialVersionUID = -1086825204860145543L;

   private static final Logger log = Logger.getLogger(UDPServerLocatorImpl.class);

   private String discoveryGroupName;

   private InetAddress localBindAddress;

   private InetAddress groupAddress;

   private int groupPort;

   private long refreshTimeout;

   private long initialWaitTimeout;

   private DiscoveryGroup discoveryGroup;

   private final Exception e = new Exception();

   @Override
   protected synchronized void initialiseInternal() throws Exception
   {
      discoveryGroupName = getDiscoveryGroupConfiguration().getName();

      Map<String,Object> params = getDiscoveryGroupConfiguration().getParams();

      String lbaStr = ConfigurationHelper.getStringProperty(DiscoveryGroupConstants.LOCAL_BIND_ADDRESS_NAME, null, params);

      localBindAddress = lbaStr != null ? InetAddress.getByName(lbaStr) : null;

      String graStr = ConfigurationHelper.getStringProperty(DiscoveryGroupConstants.GROUP_ADDRESS_NAME, null, params);

      groupAddress = graStr != null ? InetAddress.getByName(graStr) : null;

      groupPort = ConfigurationHelper.getIntProperty(DiscoveryGroupConstants.GROUP_PORT_NAME, -1, params);
      refreshTimeout =
               ConfigurationHelper.getLongProperty(DiscoveryGroupConstants.REFRESH_TIMEOUT_NAME,
                                                   ConfigurationImpl.DEFAULT_BROADCAST_REFRESH_TIMEOUT, params);
      initialWaitTimeout =
               ConfigurationHelper.getLongProperty(DiscoveryGroupConstants.INITIAL_WAIT_TIMEOUT_NAME,
                                                   HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT, params);

      discoveryGroup = new DiscoveryGroupImpl(getNodeID(),
                                              this.discoveryGroupName,
                                              this.localBindAddress,
                                              this.groupAddress,
                                              this.groupPort,
                                              this.refreshTimeout);

      discoveryGroup.registerListener(this);

      discoveryGroup.start();
   }

   private UDPServerLocatorImpl(final Topology topology,
                             final boolean useHA,
                             final DiscoveryGroupConfiguration discoveryGroupConfiguration,
                             final TransportConfiguration[] transportConfigs)
   {
      super(topology, useHA, discoveryGroupConfiguration, transportConfigs);

      e.fillInStackTrace();
   }

   /**
    * Create a ServerLocatorImpl using UDP discovery to lookup cluster
    *
    * @param discoveryAddress
    * @param discoveryPort
    */
   public UDPServerLocatorImpl(final boolean useHA, final DiscoveryGroupConfiguration groupConfiguration)
   {
      this(useHA ? new Topology(null) : null, useHA, groupConfiguration, null);
      if (useHA)
      {
         // We only set the owner at where the Topology was created.
         // For that reason we can't set it at the main constructor
         getTopology().setOwner(this);
      }
   }

   /**
    * Create a ServerLocatorImpl using a static list of live servers
    *
    * @param transportConfigs
    */
   public UDPServerLocatorImpl(final boolean useHA, final TransportConfiguration... transportConfigs)
   {
      this(useHA ? new Topology(null) : null, useHA, null, transportConfigs);
      if (useHA)
      {
         // We only set the owner at where the Topology was created.
         // For that reason we can't set it at the main constructor
         getTopology().setOwner(this);
      }
   }

   /**
    * Create a ServerLocatorImpl using UDP discovery to lookup cluster
    *
    * @param discoveryAddress
    * @param discoveryPort
    */
   public UDPServerLocatorImpl(final Topology topology,
                            final boolean useHA,
                            final DiscoveryGroupConfiguration groupConfiguration)
   {
      this(topology, useHA, groupConfiguration, null);

   }

   /**
    * Create a ServerLocatorImpl using a static list of live servers
    *
    * @param transportConfigs
    */
   public UDPServerLocatorImpl(final Topology topology,
                            final boolean useHA,
                            final TransportConfiguration... transportConfigs)
   {
      this(topology, useHA, null, transportConfigs);
   }

   @Override
   public ClientSessionFactoryInternal connect() throws Exception
   {
      return (ClientSessionFactoryInternal)createSessionFactory();
   }

   @Override
   protected void doCloseInternal()
   {
      try
      {
         discoveryGroup.stop();
      }
      catch (Exception e)
      {
         log.error("Failed to stop discovery group", e);
      }
   }

   @Override
   protected void waitInitialDiscovery() throws Exception
   {
      // Wait for an initial broadcast to give us at least one node in the cluster
      long timeout = isClusterConnection() ? 0 : initialWaitTimeout;
      boolean ok = discoveryGroup.waitForBroadcast(timeout);

      if (!ok)
      {
         throw new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                                    "Timed out waiting to receive initial broadcast from cluster");
      }
   }

}
