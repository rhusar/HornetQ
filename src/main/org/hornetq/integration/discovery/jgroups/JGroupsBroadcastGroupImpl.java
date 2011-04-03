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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.config.BroadcastGroupConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUIDGenerator;
import org.jgroups.JChannel;
import org.jgroups.Message;

/**
 * A JGroupsBroadcastGroupImpl
 *
 * @author "<a href=\"tm.igarashi@gmail.com\">Tomohisa Igarashi</a>"
 *
 *
 */
public class JGroupsBroadcastGroupImpl implements BroadcastGroup, Runnable
{
   private static final Logger log = Logger.getLogger(JGroupsBroadcastGroupImpl.class);

   private final String nodeID;

   private final String name;

   private final BroadcastGroupConfiguration broadcastGroupConfiguration;
   
   private final List<TransportConfiguration> connectors;

   private String jgroupsConfigurationFileName;
   
   private String jgroupsChannelName = null;
   
   private JChannel broadcastChannel;
   
   private boolean started;

   private ScheduledFuture<?> future;

   private boolean active;

   // Each broadcast group has a unique id - we use this to detect when more than one group broadcasts the same node id
   // on the network which would be an error
   private final String uniqueID;

   private NotificationService notificationService;

   public JGroupsBroadcastGroupImpl(final String nodeID,
                                    final String name,
                                    final boolean active,
                                    final BroadcastGroupConfiguration config)
   {
      this.nodeID = nodeID;

      this.name = name;

      this.active = active;

      this.broadcastGroupConfiguration = config;
      
      this.connectors = config.getConnectorList();
      
      uniqueID = UUIDGenerator.getInstance().generateStringUUID();
   }
   
   public void setNotificationService(NotificationService notificationService)
   {
      this.notificationService = notificationService;
   }

   public void start() throws Exception
   {
      if (started)
      {
         return;
      }

      Map<String,Object> params = this.broadcastGroupConfiguration.getParams();
      this.jgroupsConfigurationFileName = ConfigurationHelper.getStringProperty(BroadcastGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME, null, params);
      this.jgroupsChannelName = ConfigurationHelper.getStringProperty(BroadcastGroupConstants.JGROUPS_CHANNEL_NAME_NAME, BroadcastGroupConstants.DEFAULT_JGROUPS_CHANNEL_NAME, params);
      this.broadcastChannel = new JChannel(Thread.currentThread().getContextClassLoader().getResource(this.jgroupsConfigurationFileName));
      
      this.broadcastChannel.connect(this.jgroupsChannelName);
      
      started = true;

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));
         Notification notification = new Notification(nodeID, NotificationType.BROADCAST_GROUP_STARTED, props);
         notificationService.sendNotification(notification);
      }
   }

   public void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      if (future != null)
      {
         future.cancel(false);
         future = null;
      }
      
      if (broadcastChannel != null)
      {
         broadcastChannel.shutdown();
         broadcastChannel.close();
         broadcastChannel = null;
      }

      started = false;

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));
         Notification notification = new Notification(nodeID, NotificationType.BROADCAST_GROUP_STOPPED, props);
         try
         {
            notificationService.sendNotification(notification);
         }
         catch (Exception e)
         {
            JGroupsBroadcastGroupImpl.log.warn("unable to send notification when broadcast group is stopped", e);
         }
      }

   }

   public boolean isStarted()
   {
      return this.started;
   }

   public String getName()
   {
      return this.name;
   }

   public void addConnector(TransportConfiguration tcConfig)
   {
      this.connectors.add(tcConfig);
   }

   public void removeConnector(TransportConfiguration tcConfig)
   {
      this.connectors.remove(tcConfig);
   }

   public int size()
   {
      return this.connectors.size();
   }

   public void activate()
   {
      this.active = true;
   }

   public void broadcastConnectors() throws Exception
   {
      if (!active)
      {
         return;
      }

      HornetQBuffer buff = HornetQBuffers.dynamicBuffer(4096);

      buff.writeString(nodeID);

      buff.writeString(uniqueID);

      buff.writeInt(connectors.size());

      for (TransportConfiguration tcConfig : connectors)
      {
         tcConfig.encode(buff);
      }

      byte[] data = buff.toByteBuffer().array();

      Message msg = new Message();

      msg.setBuffer(data);

      this.broadcastChannel.send(msg);
   }

   public void run()
   {
      if (!started)
      {
         return;
      }

      try
      {
         broadcastConnectors();
      }
      catch (Exception e)
      {
         JGroupsBroadcastGroupImpl.log.error("Failed to broadcast connector configs", e);
      }
   }

   public void schedule(ScheduledExecutorService scheduler)
   {
      Map<String,Object> params = broadcastGroupConfiguration.getParams();
      
      this.future = scheduler.scheduleWithFixedDelay(this,
                                                     0L,
                                                     Long.parseLong((String)params.get(BroadcastGroupConstants.BROADCAST_PERIOD_NAME)),
                                                     TimeUnit.MILLISECONDS);
   }
}
