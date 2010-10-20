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

package org.hornetq.jms.management.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.StandardMBean;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.jms.management.ConnectionFactoryControl;
import org.hornetq.api.jms.management.JMSQueueControl;
import org.hornetq.api.jms.management.JMSServerControl;
import org.hornetq.api.jms.management.TopicControl;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.management.impl.MBeanInfoHelper;
import org.hornetq.core.server.ServerConsumer;
import org.hornetq.core.server.ServerSession;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQQueue;
import org.hornetq.jms.client.HornetQTopic;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;
import org.hornetq.jms.server.impl.JMSFactoryType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSServerControlImpl extends StandardMBean implements JMSServerControl, NotificationEmitter
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JMSServerManager server;

   private final NotificationBroadcasterSupport broadcaster;

   private final AtomicLong notifSeq = new AtomicLong(0);

   // Static --------------------------------------------------------

   private static String[] convert(final Object[] jndiBindings)
   {
      String[] bindings = new String[jndiBindings.length];
      for (int i = 0, jndiBindingsLength = jndiBindings.length; i < jndiBindingsLength; i++)
      {
         bindings[i] = jndiBindings[i].toString().trim();
      }
      return bindings;
   }

   private static String[] toArray(final String commaSeparatedString)
   {
      if (commaSeparatedString == null || commaSeparatedString.trim().length() == 0)
      {
         return new String[0];
      }
      String[] values = commaSeparatedString.split(",");
      String[] trimmed = new String[values.length];
      for (int i = 0; i < values.length; i++)
      {
         trimmed[i] = values[i].trim();
         trimmed[i] = trimmed[i].replace("&comma;", ",");
      }
      return trimmed;
   }
   
   private static String[] determineJMSDestination(String coreAddress)
   {
      String[] result = new String[2]; // destination name & type
      if (coreAddress.startsWith(HornetQQueue.JMS_QUEUE_ADDRESS_PREFIX))
      {
         result[0] = coreAddress.substring(HornetQQueue.JMS_QUEUE_ADDRESS_PREFIX.length());
         result[1] = "queue";
      }
      else if (coreAddress.startsWith(HornetQQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX))
      {
         result[0] = coreAddress.substring(HornetQQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX.length());
         result[1] = "tempqueue";
      }
      else if (coreAddress.startsWith(HornetQQueue.JMS_TOPIC_ADDRESS_PREFIX))
      {
         result[0] = coreAddress.substring(HornetQQueue.JMS_TOPIC_ADDRESS_PREFIX.length());
         result[1] = "topic";
      }
      else if (coreAddress.startsWith(HornetQQueue.JMS_TEMP_TOPIC_ADDRESS_PREFIX))
      {
         result[0] = coreAddress.substring(HornetQQueue.JMS_TEMP_TOPIC_ADDRESS_PREFIX.length());
         result[1] = "temptopic";
      }
      else
      {
         System.out.println("JMSServerControlImpl.determineJMSDestination()" + coreAddress);
         // not related to JMS
         return null;
      }
      return result;
   }

   private static List<Pair<TransportConfiguration, TransportConfiguration>> convertToConnectorPairs(final Object[] liveConnectorsTransportClassNames,
                                                                                                     final Object[] liveConnectorTransportParams,
                                                                                                     final Object[] backupConnectorsTransportClassNames,
                                                                                                     final Object[] backupConnectorTransportParams)
                                                                                                     {
      List<Pair<TransportConfiguration, TransportConfiguration>> pairs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();

      for (int i = 0; i < liveConnectorsTransportClassNames.length; i++)
      {
         Map<String, Object> liveParams = null;
         if (liveConnectorTransportParams.length > i)
         {
            liveParams = (Map<String, Object>)liveConnectorTransportParams[i];
         }

         TransportConfiguration tcLive = new TransportConfiguration(liveConnectorsTransportClassNames[i].toString(),
                                                                    liveParams);

         Map<String, Object> backupParams = null;
         if (backupConnectorTransportParams.length > i)
         {
            backupParams = (Map<String, Object>)backupConnectorTransportParams[i];
         }

         TransportConfiguration tcBackup = null;
         if (backupConnectorsTransportClassNames.length > i)
         {
            new TransportConfiguration(backupConnectorsTransportClassNames[i].toString(), backupParams);
         }
         Pair<TransportConfiguration, TransportConfiguration> pair = new Pair<TransportConfiguration, TransportConfiguration>(tcLive,
                  tcBackup);

         pairs.add(pair);
      }

      return pairs;
                                                                                                     }

   public static MBeanNotificationInfo[] getNotificationInfos()
   {
      NotificationType[] values = NotificationType.values();
      String[] names = new String[values.length];
      for (int i = 0; i < values.length; i++)
      {
         names[i] = values[i].toString();
      }
      return new MBeanNotificationInfo[] { new MBeanNotificationInfo(names,
                                                                     JMSServerControl.class.getName(),
      "Notifications emitted by a JMS Server") };
   }

   // Constructors --------------------------------------------------

   public JMSServerControlImpl(final JMSServerManager server) throws Exception
   {
      super(JMSServerControl.class);
      this.server = server;
      broadcaster = new NotificationBroadcasterSupport();
   }

   // Public --------------------------------------------------------

   // JMSServerControlMBean implementation --------------------------
   public void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final Object[] jndiBindings) throws Exception
                                       {
      checkStarted();

      clearIO();

      try
      {
         TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

         server.createConnectionFactory(name, liveTC, JMSFactoryType.CF, JMSServerControlImpl.convert(jndiBindings));

         sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
      }
      finally
      {
         blockOnIO();
      }
                                       }

   public void createXAConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final Object[] jndiBindings) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

         server.createConnectionFactory(name, liveTC, JMSFactoryType.XA_CF, JMSServerControlImpl.convert(jndiBindings));

         sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void createQueueConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final Object[] jndiBindings) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

         server.createConnectionFactory(name, liveTC, JMSFactoryType.QUEUE_CF, JMSServerControlImpl.convert(jndiBindings));

         sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void createTopicConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final Object[] jndiBindings) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

         server.createConnectionFactory(name, liveTC, JMSFactoryType.TOPIC_CF, JMSServerControlImpl.convert(jndiBindings));

         sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void createXAQueueConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final Object[] jndiBindings) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

         server.createConnectionFactory(name, liveTC, JMSFactoryType.QUEUE_XA_CF, JMSServerControlImpl.convert(jndiBindings));

         sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void createXATopicConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final Object[] jndiBindings) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         TransportConfiguration liveTC = new TransportConfiguration(liveTransportClassName, liveTransportParams);

         server.createConnectionFactory(name, liveTC, JMSFactoryType.TOPIC_XA_CF, JMSServerControlImpl.convert(jndiBindings));

         sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void createConnectionFactory(final String name,
                                       final Object[] liveConnectorsTransportClassNames,
                                       final Object[] liveConnectorTransportParams,
                                       final Object[] backupConnectorsTransportClassNames,
                                       final Object[] backupConnectorTransportParams,
                                       final Object[] jndiBindings) throws Exception
                                       {
      checkStarted();

      clearIO();

      try
      {
         List<Pair<TransportConfiguration, TransportConfiguration>> pairs = JMSServerControlImpl.convertToConnectorPairs(liveConnectorsTransportClassNames,
                                                                                                                         liveConnectorTransportParams,
                                                                                                                         backupConnectorsTransportClassNames,
                                                                                                                         backupConnectorTransportParams);
         server.createConnectionFactory(name, pairs, JMSServerControlImpl.convert(jndiBindings));

         sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
      }
      finally
      {
         blockOnIO();
      }
                                       }

   public void createConnectionFactory(final String name,
                                       final String liveTransportClassNames,
                                       final String liveTransportParams,
                                       final String backupTransportClassNames,
                                       final String backupTransportParams,
                                       final String jndiBindings) throws Exception
                                       {
      checkStarted();

      clearIO();

      try
      {
         Object[] liveClassNames = JMSServerControlImpl.toArray(liveTransportClassNames);
         Object[] liveParams = ManagementHelper.fromCommaSeparatedArrayOfCommaSeparatedKeyValues(liveTransportParams);
         Object[] backupClassNames = JMSServerControlImpl.toArray(backupTransportClassNames);
         Object[] backupParams = ManagementHelper.fromCommaSeparatedArrayOfCommaSeparatedKeyValues(backupTransportParams);;
         Object[] bindings = JMSServerControlImpl.toArray(jndiBindings);
         createConnectionFactory(name, liveClassNames, liveParams, backupClassNames, backupParams, bindings);
      }
      finally
      {
         blockOnIO();
      }
                                       }



   public void createConnectionFactory(final String name,
                                       final String discoveryAddress,
                                       final int discoveryPort,
                                       final Object[] jndiBindings) throws Exception
                                       {
      checkStarted();

      clearIO();

      try
      {
         server.createConnectionFactory(name, discoveryAddress, discoveryPort, JMSServerControlImpl.convert(jndiBindings));

         sendNotification(NotificationType.CONNECTION_FACTORY_CREATED, name);
      }
      finally
      {
         blockOnIO();
      }
                                       }

   public void createConnectionFactory(final String name,
                                       final String discoveryAddress,
                                       final int discoveryPort,
                                       final String jndiBindings) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         Object[] bindings = JMSServerControlImpl.toArray(jndiBindings);

         createConnectionFactory(name, discoveryAddress, discoveryPort, bindings);

      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean createQueue(String name) throws Exception
   {
      return createQueue(name, null, null, true);
   }

   public boolean createQueue(final String name, final String jndiBindings) throws Exception
   {
      return createQueue(name, jndiBindings, null, true);
   }

   public boolean createQueue(String name, String jndiBindings, String selector) throws Exception
   {
      return createQueue(name, jndiBindings, selector, true);
   }
   
   public boolean createQueue(String name, String jndiBindings, String selector, boolean durable) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         boolean created = server.createQueue(true, name, selector, durable, JMSServerControlImpl.toArray(jndiBindings));
         if (created)
         {
            sendNotification(NotificationType.QUEUE_CREATED, name);
         }
         return created;
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean destroyQueue(final String name) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         boolean destroyed = server.destroyQueue(name);
         if (destroyed)
         {
            sendNotification(NotificationType.QUEUE_DESTROYED, name);
         }
         return destroyed;
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean createTopic(String name) throws Exception
   {
      return createTopic(name, null);
   }

   public boolean createTopic(final String topicName, final String jndiBindings) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         boolean created = server.createTopic(true, topicName, JMSServerControlImpl.toArray(jndiBindings));
         if (created)
         {
            sendNotification(NotificationType.TOPIC_CREATED, topicName);
         }
         return created;
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean destroyTopic(final String name) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         boolean destroyed = server.destroyTopic(name);
         if (destroyed)
         {
            sendNotification(NotificationType.TOPIC_DESTROYED, name);
         }
         return destroyed;
      }
      finally
      {
         blockOnIO();
      }
   }

   public void destroyConnectionFactory(final String name) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         boolean destroyed = server.destroyConnectionFactory(name);
         if (destroyed)
         {
            sendNotification(NotificationType.CONNECTION_FACTORY_DESTROYED, name);
         }
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isStarted()
   {
      return server.isStarted();
   }

   public String getVersion()
   {
      checkStarted();

      return server.getVersion();
   }

   public String[] getQueueNames()
   {
      checkStarted();

      clearIO();

      try
      {
         Object[] queueControls = server.getHornetQServer().getManagementService().getResources(JMSQueueControl.class);
         String[] names = new String[queueControls.length];
         for (int i = 0; i < queueControls.length; i++)
         {
            JMSQueueControl queueControl = (JMSQueueControl)queueControls[i];
            names[i] = queueControl.getName();
         }
         return names;
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] getTopicNames()
   {
      checkStarted();

      clearIO();

      try
      {
         Object[] topicControls = server.getHornetQServer().getManagementService().getResources(TopicControl.class);
         String[] names = new String[topicControls.length];
         for (int i = 0; i < topicControls.length; i++)
         {
            TopicControl topicControl = (TopicControl)topicControls[i];
            names[i] = topicControl.getName();
         }
         return names;
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] getConnectionFactoryNames()
   {
      checkStarted();

      clearIO();

      try
      {
         Object[] cfControls = server.getHornetQServer()
         .getManagementService()
         .getResources(ConnectionFactoryControl.class);
         String[] names = new String[cfControls.length];
         for (int i = 0; i < cfControls.length; i++)
         {
            ConnectionFactoryControl cfControl = (ConnectionFactoryControl)cfControls[i];
            names[i] = cfControl.getName();
         }
         return names;
      }
      finally
      {
         blockOnIO();
      }
   }

   // NotificationEmitter implementation ----------------------------

   public void removeNotificationListener(final NotificationListener listener,
                                          final NotificationFilter filter,
                                          final Object handback) throws ListenerNotFoundException
                                          {
      broadcaster.removeNotificationListener(listener, filter, handback);
                                          }

   public void removeNotificationListener(final NotificationListener listener) throws ListenerNotFoundException
   {
      broadcaster.removeNotificationListener(listener);
   }

   public void addNotificationListener(final NotificationListener listener,
                                       final NotificationFilter filter,
                                       final Object handback) throws IllegalArgumentException
                                       {
      broadcaster.addNotificationListener(listener, filter, handback);
                                       }

   public MBeanNotificationInfo[] getNotificationInfo()
   {
      return JMSServerControlImpl.getNotificationInfos();
   }

   public String[] listRemoteAddresses() throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.listRemoteAddresses();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] listRemoteAddresses(final String ipAddress) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.listRemoteAddresses(ipAddress);
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.closeConnectionsForAddress(ipAddress);
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] listConnectionIDs() throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.listConnectionIDs();
      }
      finally
      {
         blockOnIO();
      }
   }
   
   public String listConnectionsAsJSON() throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         JSONArray array = new JSONArray();
         
         Set<RemotingConnection> connections = server.getHornetQServer().getRemotingService().getConnections();

         Set<ServerSession> sessions = server.getHornetQServer().getSessions();
         
         Map<Object, ServerSession> jmsSessions = new HashMap<Object, ServerSession>();

         for (ServerSession session : sessions)
         {
            if (session.getMetaData("jms-client-id") != null)
            {
               jmsSessions.put(session.getConnectionID(), session);
            }
         }

         for (RemotingConnection connection : connections)
         {
            JSONObject obj = new JSONObject();
            obj.put("connectionID", connection.getID().toString());
            obj.put("clientAddress", connection.getRemoteAddress());
            obj.put("creationTime", connection.getCreationTime());
            obj.put("clientID", jmsSessions.get(connection.getID()).getMetaData("jms-client-id"));
            obj.put("principal", jmsSessions.get(connection.getID()).getUsername());
            array.put(obj);
         }
         return array.toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String listConsumersAsJSON(String connectionID) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         JSONArray array = new JSONArray();
         
         Set<RemotingConnection> connections = server.getHornetQServer().getRemotingService().getConnections();
         for (RemotingConnection connection : connections)
         {
            if (connectionID.equals(connection.getID().toString()))
            {
               List<ServerSession> sessions = server.getHornetQServer().getSessions(connectionID);
               for (ServerSession session : sessions)
               {
                  Set<ServerConsumer> consumers = session.getServerConsumers();
                  for (ServerConsumer consumer : consumers)
                  {
                     JSONObject obj = new JSONObject();
                     obj.put("consumerID", consumer.getID());
                     obj.put("connectionID", connectionID);
                     obj.put("queueName", consumer.getQueue().getName().toString());
                     obj.put("browseOnly", consumer.isBrowseOnly());
                     obj.put("creationTime", consumer.getCreationTime());
                     // JMS consumer with message filter use the queue's filter
                     Filter queueFilter = consumer.getQueue().getFilter();
                     if (queueFilter != null)
                     {
                        obj.put("filter", queueFilter.getFilterString().toString());
                     }
                     String[] destinationInfo = determineJMSDestination(consumer.getQueue().getAddress().toString());
                     if (destinationInfo == null)
                     {
                        continue;
                     }
                     obj.put("destinationName", destinationInfo[0]);
                     obj.put("destinationType", destinationInfo[1]);
                     if (destinationInfo[1].equals("topic")) {
                        try
                        {
                           HornetQDestination.decomposeQueueNameForDurableSubscription(consumer.getQueue().getName().toString());
                           obj.put("durable", true);
                        } catch (IllegalArgumentException e)
                        {
                           obj.put("durable", false);
                        }
                     }
                     else
                     {
                        obj.put("durable", false);
                     }
                     array.put(obj);
                  }
               }
            }
         }
         return array.toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String[] listSessions(final String connectionID) throws Exception
   {
      checkStarted();

      clearIO();

      try
      {
         return server.listSessions(connectionID);
      }
      finally
      {
         blockOnIO();
      }
   }

   @Override
   public MBeanInfo getMBeanInfo()
   {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(),
                           info.getDescription(),
                           info.getAttributes(),
                           info.getConstructors(),
                           MBeanInfoHelper.getMBeanOperationsInfo(JMSServerControl.class),
                           info.getNotifications());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void sendNotification(final NotificationType type, final String message)
   {
      Notification notif = new Notification(type.toString(), this, notifSeq.incrementAndGet(), message);
      broadcaster.sendNotification(notif);
   }

   private void checkStarted()
   {
      if (!server.isStarted())
      {
         throw new IllegalStateException("HornetQ JMS Server is not started. it can not be managed yet");
      }
   }

   protected void clearIO()
   {
      // the storage manager could be null on the backup on certain components
      if (server.getHornetQServer().getStorageManager() != null)
      {
         server.getHornetQServer().getStorageManager().clearContext();
      }
   }

   protected void blockOnIO()
   {
      // the storage manager could be null on the backup on certain components
      if (server.getHornetQServer().getStorageManager() != null)
      {
         try
         {
            server.getHornetQServer().getStorageManager().waitOnOperations();
            server.getHornetQServer().getStorageManager().clearContext();
         }
         catch (Exception e)
         {
            throw new RuntimeException(e.getMessage(), e);
         }
      }

   }

   // Inner classes -------------------------------------------------

   public static enum NotificationType
   {
      QUEUE_CREATED,
      QUEUE_DESTROYED,
      TOPIC_CREATED,
      TOPIC_DESTROYED,
      CONNECTION_FACTORY_CREATED,
      CONNECTION_FACTORY_DESTROYED;
   }

   private static List<String> toList(final String commaSeparatedString)
   {
      List<String> list = new ArrayList<String>();
      if (commaSeparatedString == null || commaSeparatedString.trim().length() == 0)
      {
         return list;
      }
      String[] values = commaSeparatedString.split(",");
      for (int i = 0; i < values.length; i++)
      {
         list.add(values[i].trim());
      }
      return list;
   }
}
