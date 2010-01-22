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

package org.hornetq.core.remoting.server.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.aardvark.impl.AardvarkProtocolManagerFactory;
import org.hornetq.core.protocol.core.impl.CoreProtocolManagerFactory;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.integration.transports.netty.ServerHolder;
import org.hornetq.spi.core.protocol.ConnectionEntry;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.AcceptorFactory;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.utils.ConfigurationHelper;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class RemotingServiceImpl implements RemotingService, ConnectionLifeCycleListener
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingServiceImpl.class);

   public static final long CONNECTION_TTL_CHECK_INTERVAL = 2000;

   // Attributes ----------------------------------------------------

   private volatile boolean started = false;

   private final Set<TransportConfiguration> transportConfigs;

   private final List<Interceptor> interceptors = new CopyOnWriteArrayList<Interceptor>();

   private final Set<Acceptor> acceptors = new HashSet<Acceptor>();

   private final Map<Object, ConnectionEntry> connections = new ConcurrentHashMap<Object, ConnectionEntry>();

   //private final BufferHandler bufferHandler = new DelegatingBufferHandler();

   private final Configuration config;

   private final HornetQServer server;

   private final ManagementService managementService;

   private volatile RemotingConnection serverSideReplicatingConnection;

   private final Executor threadPool;

   private final ScheduledExecutorService scheduledThreadPool;

   private FailureCheckAndFlushThread failureCheckAndFlushThread;
   
   private Map<ProtocolType, ProtocolManager> protocolMap = 
      new ConcurrentHashMap<ProtocolType, ProtocolManager>();
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RemotingServiceImpl(final Configuration config,
                              final HornetQServer server,
                              final ManagementService managementService,
                              final Executor threadPool,
                              final ScheduledExecutorService scheduledThreadPool)
   {
      transportConfigs = config.getAcceptorConfigurations();

      this.server = server;

      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      for (String interceptorClass : config.getInterceptorClassNames())
      {
         try
         {
            Class<?> clazz = loader.loadClass(interceptorClass);
            interceptors.add((Interceptor)clazz.newInstance());
         }
         catch (Exception e)
         {
            RemotingServiceImpl.log.warn("Error instantiating interceptor \"" + interceptorClass + "\"", e);
         }
      }

      this.config = config;
      this.managementService = managementService;
      this.threadPool = threadPool;
      this.scheduledThreadPool = scheduledThreadPool;
      
      this.protocolMap.put(ProtocolType.CORE, new CoreProtocolManagerFactory().createProtocolManager(server, interceptors));
      this.protocolMap.put(ProtocolType.AARDVARK, new AardvarkProtocolManagerFactory().createProtocolManager(server, interceptors));
   }

   // RemotingService implementation -------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      ClassLoader loader = Thread.currentThread().getContextClassLoader();

      for (TransportConfiguration info : transportConfigs)
      {
         try
         {
            Class<?> clazz = loader.loadClass(info.getFactoryClassName());

            AcceptorFactory factory = (AcceptorFactory)clazz.newInstance();

            // Check valid properties

            if (info.getParams() != null)
            {
               Set<String> invalid = ConfigurationHelper.checkKeys(factory.getAllowableProperties(), info.getParams()
                                                                                                         .keySet());

               if (!invalid.isEmpty())
               {
                  RemotingServiceImpl.log.warn(ConfigurationHelper.stringSetToCommaListString("The following keys are invalid for configuring the acceptor: ",
                                                                                              invalid) + " the acceptor will not be started.");

                  continue;
               }
            }
            
            //TODO - allow protocol type to be configured from Configuration for each acceptor

            ProtocolType protocol = hackProtocol;
            
            ProtocolManager manager = protocolMap.get(protocol);
            
            Acceptor acceptor = factory.createAcceptor(info.getParams(),
                                                       new DelegatingBufferHandler(manager),
                                                       new ServerHolder()
            {
               public HornetQServer getServer()
               {
                  return server;
               }
               
               public RemotingConnection getRemotingConnection(int connectionID)
               {
                  ConnectionEntry conn = connections.get(connectionID);

                  if (conn != null)
                  {
                     return conn.connection;
                  }
                  else
                  {
                     return null;
                  }
               }
            },
                                                       this,
                                                       threadPool,
                                                       scheduledThreadPool);

            acceptors.add(acceptor);

            if (managementService != null)
            {
               acceptor.setNotificationService(managementService);
               
               managementService.registerAcceptor(acceptor, info);
            }
         }
         catch (Exception e)
         {
            RemotingServiceImpl.log.warn("Error instantiating acceptor \"" + info.getFactoryClassName() + "\"", e);
         }
      }

      for (Acceptor a : acceptors)
      {
         a.start();
      }

      // This thread checks connections that need to be closed, and also flushes confirmations
      failureCheckAndFlushThread = new FailureCheckAndFlushThread(RemotingServiceImpl.CONNECTION_TTL_CHECK_INTERVAL);

      failureCheckAndFlushThread.start();

      started = true;
   }
   
   //FIXME - temp hack so we can choose AARDVARK as protocol
   public static ProtocolType hackProtocol = ProtocolType.CORE;

   public synchronized void freeze()
   {
      // Used in testing - prevents service taking any more connections

      for (Acceptor acceptor : acceptors)
      {
         try
         {
            acceptor.pause();
         }
         catch (Exception e)
         {
            RemotingServiceImpl.log.error("Failed to stop acceptor", e);
         }
      }
   }

   public void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      if (!started)
      {
         return;
      }

      failureCheckAndFlushThread.close();

      // We need to stop them accepting first so no new connections are accepted after we send the disconnect message
      for (Acceptor acceptor : acceptors)
      {
         acceptor.pause();
      }

      // Now we ensure that no connections will process any more packets after this method is complete
      // then send a disconnect packet
      for (ConnectionEntry entry : connections.values())
      {
         RemotingConnection conn = entry.connection;

         conn.disconnect();
      }

      for (Acceptor acceptor : acceptors)
      {
         acceptor.stop();
      }

      acceptors.clear();

      connections.clear();

      if (managementService != null)
      {
         managementService.unregisterAcceptors();
      }

      started = false;

   }

   public boolean isStarted()
   {
      return started;
   }

   public RemotingConnection removeConnection(final Object remotingConnectionID)
   {
      ConnectionEntry entry = connections.remove(remotingConnectionID);

      if (entry != null)
      {
         return entry.connection;
      }
      else
      {
         return null;
      }
   }

   public synchronized Set<RemotingConnection> getConnections()
   {
      Set<RemotingConnection> conns = new HashSet<RemotingConnection>();

      for (ConnectionEntry entry : connections.values())
      {
         conns.add(entry.connection);
      }

      return conns;
   }

   public RemotingConnection getServerSideReplicatingConnection()
   {
      return serverSideReplicatingConnection;
   }

   // ConnectionLifeCycleListener implementation -----------------------------------

   private ProtocolManager getProtocolManager(ProtocolType protocol)
   {
      return protocolMap.get(protocol);
   }
   
   public void connectionCreated(final Connection connection, final ProtocolType protocol)
   {
      if (server == null)
      {
         throw new IllegalStateException("Unable to create connection, server hasn't finished starting up");
      }
      
      ProtocolManager pmgr = this.getProtocolManager(protocol);
      
      if (pmgr == null)
      {
         throw new IllegalArgumentException("Unknown protocol " + protocol);
      }

      ConnectionEntry entry = pmgr.createConnectionEntry(connection);
      
      connections.put(connection.getID(), entry);

      if (config.isBackup())
      {
         serverSideReplicatingConnection = entry.connection;
      }
   }

   public void connectionDestroyed(final Object connectionID)
   {
      ConnectionEntry conn = connections.get(connectionID);

      if (conn != null)
      {
         // if the connection has no failure listeners it means the sesssions etc were already closed so this is a clean
         // shutdown, therefore we can destroy the connection
         // otherwise client might have crashed/exited without closing connections so we leave them for connection TTL

         if (conn.connection.getFailureListeners().isEmpty())
         {
            connections.remove(connectionID);

            conn.connection.destroy();
         }
      }
   }

   public void connectionException(final Object connectionID, final HornetQException me)
   {
      // We DO NOT call fail on connection exception, otherwise in event of real connection failure, the
      // connection will be failed, the session will be closed and won't be able to reconnect

      // E.g. if live server fails, then this handler wil be called on backup server for the server
      // side replicating connection.
      // If the connection fail() is called then the sessions on the backup will get closed.

      // Connections should only fail when TTL is exceeded
   }

   public void addInterceptor(final Interceptor interceptor)
   {
      interceptors.add(interceptor);
   }

   public boolean removeInterceptor(final Interceptor interceptor)
   {
      return interceptors.remove(interceptor);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private final class DelegatingBufferHandler implements BufferHandler
   {
      private ProtocolManager manager;
      
      DelegatingBufferHandler(final ProtocolManager manager)
      {
         this.manager = manager;
      }
      
      public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
      {
         ConnectionEntry conn = connections.get(connectionID);

         if (conn != null)
         {
            conn.connection.bufferReceived(connectionID, buffer);
         }
      }

      public int isReadyToHandle(HornetQBuffer buffer)
      {
         return manager.isReadyToHandle(buffer);
      }
   }

   private final class FailureCheckAndFlushThread extends Thread
   {
      private final long pauseInterval;

      private volatile boolean closed;

      FailureCheckAndFlushThread(final long pauseInterval)
      {
         super("hornetq-failure-check-thread");

         this.pauseInterval = pauseInterval;
      }

      public synchronized void close()
      {
         closed = true;

         synchronized (this)
         {
            notify();
         }

         try
         {
            join();
         }
         catch (InterruptedException ignore)
         {
         }
      }

      @Override
      public void run()
      {
         while (!closed)
         {
            long now = System.currentTimeMillis();

            Set<Object> idsToRemove = new HashSet<Object>();

            for (ConnectionEntry entry : connections.values())
            {
               RemotingConnection conn = entry.connection;

               boolean flush = true;

               if (entry.ttl != -1)
               {
                  if (now >= entry.lastCheck + entry.ttl)
                  {
                     if (!conn.checkDataReceived())
                     {
                        idsToRemove.add(conn.getID());

                        flush = false;
                     }
                     else
                     {
                        entry.lastCheck = now;
                     }
                  }
               }

               if (flush)
               {                                   
                  conn.flush();
               }
            }

            for (Object id : idsToRemove)
            {
               RemotingConnection conn = removeConnection(id);

               HornetQException me = new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                                                          "Did not receive ping from " + conn.getRemoteAddress() +
                                                                   ". It is likely the client has exited or crashed without " +
                                                                   "closing its connection, or the network between the server and client has failed. The connection will now be closed.");
               conn.fail(me);
            }

            synchronized (this)
            {
               long toWait = pauseInterval;

               long start = System.currentTimeMillis();

               while (!closed && toWait > 0)
               {
                  try
                  {
                     wait(toWait);
                  }
                  catch (InterruptedException e)
                  {
                  }

                  now = System.currentTimeMillis();

                  toWait -= now - start;

                  start = now;
               }
            }
         }
      }
   }
}