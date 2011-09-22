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

import java.lang.reflect.Array;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConstants;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.cluster.impl.DiscoveryGroupImpl;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.ConfigurationHelper;

/**
 * A SimpleUDPServerLocatorImpl, was derived from ServerLocatorImpl
 *
 * @author Tim Fox
 * @author <a href="tm.igarashi@gmail.com">Tomohisa Igarashi</a>
 */
public class SimpleUDPServerLocatorImpl extends AbstractServerLocator
{
   private static final Logger log = Logger.getLogger(SimpleUDPServerLocatorImpl.class);

   private String discoveryGroupName;
     
   private InetAddress localBindAddress;
      
   private InetAddress groupAddress;
      
   private int groupPort;
      
   private long refreshTimeout;
      
   private long initialWaitTimeout;
      
   private DiscoveryGroup discoveryGroup;
   
   private volatile boolean closing;

   @Override
   protected synchronized void initialiseInternal() throws Exception
   {
      this.discoveryGroupName = getDiscoveryGroupConfiguration().getName();
         
      Map<String,Object> params = getDiscoveryGroupConfiguration().getParams();
         
      String lbStr = ConfigurationHelper.getStringProperty(DiscoveryGroupConstants.LOCAL_BIND_ADDRESS_NAME, null, params);
            
      if (lbStr != null)
      {
         this.localBindAddress = InetAddress.getByName(lbStr);
      }
      else
      {
         this.localBindAddress = null;
      }

      String gaddr = ConfigurationHelper.getStringProperty(DiscoveryGroupConstants.GROUP_ADDRESS_NAME, null, params);
      if(gaddr != null)
      {
         this.groupAddress = InetAddress.getByName(gaddr);
      }
      this.groupPort = ConfigurationHelper.getIntProperty(DiscoveryGroupConstants.GROUP_PORT_NAME, -1, params);
      this.refreshTimeout = ConfigurationHelper.getLongProperty(DiscoveryGroupConstants.REFRESH_TIMEOUT_NAME, ConfigurationImpl.DEFAULT_BROADCAST_REFRESH_TIMEOUT, params);
      this.initialWaitTimeout = ConfigurationHelper.getLongProperty(DiscoveryGroupConstants.INITIAL_WAIT_TIMEOUT_NAME, HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT, params);
         
      discoveryGroup = new DiscoveryGroupImpl(getNodeID(),
                                              this.discoveryGroupName,
                                              this.localBindAddress,
                                              this.groupAddress,
                                              this.groupPort,
                                              this.refreshTimeout);
            
      discoveryGroup.registerListener(this);
            
      discoveryGroup.start();
   }
   
   public SimpleUDPServerLocatorImpl(final boolean useHA,
                                     final DiscoveryGroupConfiguration discoveryGroupConfiguration)
   {
      super(useHA, discoveryGroupConfiguration);
   }

   public ClientSessionFactoryInternal connect() throws Exception
   {
      ClientSessionFactoryInternal sf;
      sf = (ClientSessionFactoryInternal)createSessionFactory();

      addFactory(sf);
      return sf;
   }

   @Override
   protected void waitInitialDiscovery() throws Exception
   {
      // Wait for an initial broadcast to give us at least one node in the cluster
      long timeout = this.isClusterConnection() ? 0 : this.initialWaitTimeout;
      boolean ok = discoveryGroup.waitForBroadcast(timeout);

      if (!ok)
      {
         throw new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                                    "Timed out waiting to receive initial broadcast from cluster");
      }
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
}
