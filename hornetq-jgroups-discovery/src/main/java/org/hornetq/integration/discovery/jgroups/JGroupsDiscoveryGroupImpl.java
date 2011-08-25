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

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.utils.TypedProperties;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;

/**
 * A JGroupsDiscoveryGroupImpl
 *
 * @author "<a href=\"tm.igarashi@gmail.com\">Tomohisa Igarashi</a>"
 *
 *
 */
public class JGroupsDiscoveryGroupImpl extends ReceiverAdapter implements DiscoveryGroup
{
   private static final Logger log = Logger.getLogger(JGroupsDiscoveryGroupImpl.class);

   private final List<DiscoveryListener> listeners = new ArrayList<DiscoveryListener>();

   private final String name;
   
   private final String jgroupsChannelName;

   private final URL configURL;
   
   private final String nodeID;

   private volatile boolean started;

   private boolean received;

   private final Object waitLock = new Object();

   private final Map<String, DiscoveryEntry> connectors = new HashMap<String, DiscoveryEntry>();

   private final long timeout;

   private final Map<String, String> uniqueIDMap = new HashMap<String, String>();

   private JChannel discoveryChannel;
   
   private NotificationService notificationService;
   
   public JGroupsDiscoveryGroupImpl(final String nodeID,
                                    final String name,
                                    final String channelName,
                                    final URL confURL,
                                    final long timeout)
   {
      this.nodeID = nodeID;
      this.name = name;
      this.jgroupsChannelName = channelName;
      this.configURL = confURL;
      this.timeout = timeout;
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

      try
      {
         this.discoveryChannel = new JChannel(configURL);

         this.discoveryChannel.setReceiver(this);
         
         this.discoveryChannel.connect(this.jgroupsChannelName);
      }
      catch(Exception e)
      {
         log.error("Failed to join jgroups channel", e);
         return;
      }
      
      started = true;

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));

         Notification notification = new Notification(nodeID, NotificationType.DISCOVERY_GROUP_STARTED, props);

         notificationService.sendNotification(notification);
      }
   }

   public void stop() throws Exception
   {
      synchronized (this)
      {
         if (!started)
         {
            return;
         }

         started = false;
      }

      synchronized (waitLock)
      {
         waitLock.notify();
      }

      this.discoveryChannel.shutdown();

      this.discoveryChannel.close();
      
      this.discoveryChannel = null;

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));
         Notification notification = new Notification(nodeID, NotificationType.DISCOVERY_GROUP_STOPPED, props);
         try
         {
            notificationService.sendNotification(notification);
         }
         catch (Exception e)
         {
            JGroupsDiscoveryGroupImpl.log.warn("unable to send notification when discovery group is stopped", e);
         }
      }
   }

   public String getName()
   {
      return this.name;
   }

   public String getJGroupsChannelName()
   {
      return this.jgroupsChannelName;
   }
   
   public List<DiscoveryEntry> getDiscoveryEntries()
   {
      List<DiscoveryEntry> list = new ArrayList<DiscoveryEntry>();
      
      list.addAll(connectors.values());
      
      return list;
   }

   public boolean isStarted()
   {
      return this.started;
   }

   public boolean waitForBroadcast(long timeout)
   {
      synchronized (waitLock)
      {
         long start = System.currentTimeMillis();

         long toWait = timeout;

         while (started && !received && (toWait > 0 || timeout == 0))
         {
            try
            {
               waitLock.wait(toWait);
            }
            catch (InterruptedException e)
            {
            }

            if (timeout != 0)
            {
               long now = System.currentTimeMillis();

               toWait -= now - start;

               start = now;
            }
         }

         boolean ret = received;

         received = false;

         return ret;
      }
   }

   @Override
   public void receive(Message msg)
   {
      if(!started)
      {
         return;
      }

      HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(msg.getBuffer());

      String originatingNodeID = buffer.readString();

      String uniqueID = buffer.readString();

      checkUniqueID(originatingNodeID, uniqueID);

      if (nodeID.equals(originatingNodeID))
      {
         if (checkExpiration())
         {
            callListeners();
         }
         
         // Ignore traffic from own node
         return;
      }

      int size = buffer.readInt();

      boolean changed = false;

      synchronized (this)
      {
         for (int i = 0; i < size; i++)
         {
            TransportConfiguration connector = new TransportConfiguration();

            connector.decode(buffer);
           
            DiscoveryEntry entry = new DiscoveryEntry(originatingNodeID, connector, System.currentTimeMillis());

            DiscoveryEntry oldVal = connectors.put(originatingNodeID, entry);

            if (oldVal == null)
            {
               changed = true;
            }
         }

         changed = changed || checkExpiration();
      }

      if (changed)
      {
         callListeners();
      }

      synchronized (waitLock)
      {
         received = true;

         waitLock.notify();
      }
   }
   
   public void registerListener(DiscoveryListener listener)
   {
      listeners.add(listener);

      if (!connectors.isEmpty())
      {
         listener.connectorsChanged();
      }
   }

   public void unregisterListener(DiscoveryListener listener)
   {
      listeners.remove(listener);
   }

   private void callListeners()
   {
      for (DiscoveryListener listener : listeners)
      {
         try
         {
            listener.connectorsChanged();
         }
         catch (Throwable t)
         {
            // Catch it so exception doesn't prevent other listeners from running
            JGroupsDiscoveryGroupImpl.log.error("Failed to call discovery listener", t);
         }
      }
   }
   
   private void checkUniqueID(final String originatingNodeID, final String uniqueID)
   {
      String currentUniqueID = uniqueIDMap.get(originatingNodeID);

      if (currentUniqueID == null)
      {
         uniqueIDMap.put(originatingNodeID, uniqueID);
      }
      else
      {
         if (!currentUniqueID.equals(uniqueID))
         {
            log.warn("There are more than one servers on the network broadcasting the same node id. " + "You will see this message exactly once (per node) if a node is restarted, in which case it can be safely "
                     + "ignored. But if it is logged continuously it means you really do have more than one node on the same network "
                     + "active concurrently with the same node id. This could occur if you have a backup node active at the same time as "
                     + "its live node. nodeID=" + originatingNodeID);
            uniqueIDMap.put(originatingNodeID, uniqueID);
         }
      }
   }

   private boolean checkExpiration()
   {
      boolean changed = false;
      long now = System.currentTimeMillis();

      Iterator<Map.Entry<String, DiscoveryEntry>> iter = connectors.entrySet().iterator();

      // Weed out any expired connectors

      while (iter.hasNext())
      {
         Map.Entry<String, DiscoveryEntry> entry = iter.next();

         if (entry.getValue().getLastUpdate() + timeout <= now)
         {
            iter.remove();

            changed = true;
         }
      }
      
      return changed;
   }

}
