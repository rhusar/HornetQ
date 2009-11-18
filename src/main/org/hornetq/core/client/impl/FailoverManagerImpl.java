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

package org.hornetq.core.client.impl;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.SessionFailureListener;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.AbstractBufferHandler;
import org.hornetq.core.remoting.impl.RemotingConnectionImpl;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionMessage;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.Ping;
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.remoting.spi.ConnectionLifeCycleListener;
import org.hornetq.core.remoting.spi.Connector;
import org.hornetq.core.remoting.spi.ConnectorFactory;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.version.Version;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.OrderedExecutorFactory;
import org.hornetq.utils.UUIDGenerator;
import org.hornetq.utils.VersionLoader;

/**
 * A ConnectionManagerImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 27 Nov 2008 18:46:06
 *
 */
public class FailoverManagerImpl implements FailoverManager, ConnectionLifeCycleListener
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;

   private static final Logger log = Logger.getLogger(FailoverManagerImpl.class);

   // Attributes
   // -----------------------------------------------------------------------------------

   // We hold this reference for GC reasons
   private final ClientSessionFactory sessionFactory;

   private final TransportConfiguration connectorConfig;

   private final TransportConfiguration backupConfig;

   private ConnectorFactory connectorFactory;

   private Map<String, Object> transportParams;

   private ConnectorFactory backupConnectorFactory;

   private Map<String, Object> backupTransportParams;

   private final long callTimeout;

   private final long clientFailureCheckPeriod;

   private final long connectionTTL;

   private final Set<ClientSessionInternal> sessions = new HashSet<ClientSessionInternal>();

   private final Object exitLock = new Object();

   private final Object createSessionLock = new Object();

   private boolean inCreateSession;

   private final Object failoverLock = new Object();

   private final ExecutorFactory orderedExecutorFactory;

   private final ExecutorService threadPool;

   private final ScheduledExecutorService scheduledThreadPool;

   private RemotingConnection connection;

   private final long retryInterval;

   private final double retryIntervalMultiplier; // For exponential backoff

   private final long maxRetryInterval;

   private final int reconnectAttempts;

   private boolean failoverOnServerShutdown;

   private Set<SessionFailureListener> listeners = new ConcurrentHashSet<SessionFailureListener>();

   private Connector connector;

   private Future<?> pingerFuture;

   private PingRunnable pingRunnable;

   private volatile boolean exitLoop;

   private final List<Interceptor> interceptors;

   // Static
   // ---------------------------------------------------------------------------------------

   // Constructors
   // ---------------------------------------------------------------------------------

   public FailoverManagerImpl(final ClientSessionFactory sessionFactory,
                              final TransportConfiguration connectorConfig,
                              final TransportConfiguration backupConfig,
                              final boolean failoverOnServerShutdown,
                              final long callTimeout,
                              final long clientFailureCheckPeriod,
                              final long connectionTTL,
                              final long retryInterval,
                              final double retryIntervalMultiplier,
                              final long maxRetryInterval,
                              final int reconnectAttempts,
                              final ExecutorService threadPool,
                              final ScheduledExecutorService scheduledThreadPool,
                              final List<Interceptor> interceptors)
   {
      this.sessionFactory = sessionFactory;

      this.connectorConfig = connectorConfig;

      this.backupConfig = backupConfig;

      this.failoverOnServerShutdown = failoverOnServerShutdown;

      connectorFactory = instantiateConnectorFactory(connectorConfig.getFactoryClassName());

      transportParams = connectorConfig.getParams();

      checkTransportKeys(connectorFactory, transportParams);

      if (backupConfig != null)
      {
         backupConnectorFactory = instantiateConnectorFactory(backupConfig.getFactoryClassName());

         backupTransportParams = backupConfig.getParams();

         checkTransportKeys(backupConnectorFactory, backupTransportParams);
      }
      else
      {
         backupConnectorFactory = null;

         backupTransportParams = null;
      }

      this.callTimeout = callTimeout;

      this.clientFailureCheckPeriod = clientFailureCheckPeriod;

      this.connectionTTL = connectionTTL;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetryInterval = maxRetryInterval;

      this.reconnectAttempts = reconnectAttempts;

      this.scheduledThreadPool = scheduledThreadPool;

      this.threadPool = threadPool;

      this.orderedExecutorFactory = new OrderedExecutorFactory(threadPool);

      this.interceptors = interceptors;
   }

   // ConnectionLifeCycleListener implementation --------------------------------------------------

   public void connectionCreated(final Connection connection)
   {
   }

   public void connectionDestroyed(final Object connectionID)
   {
      this.handleConnectionFailure(connectionID, new HornetQException(HornetQException.NOT_CONNECTED,
                                                                      "Channel disconnected"));
   }

   public void connectionException(final Object connectionID, final HornetQException me)
   {
      this.handleConnectionFailure(connectionID, me);
   }

   // ConnectionManager implementation ------------------------------------------------------------------

   public ClientSession createSession(final String username,
                                      final String password,
                                      final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge,
                                      final int ackBatchSize,
                                      final boolean cacheLargeMessageClient,
                                      final int minLargeMessageSize,
                                      final boolean blockOnAcknowledge,
                                      final boolean autoGroup,
                                      final int confWindowSize,
                                      final int producerWindowSize,
                                      final int consumerWindowSize,
                                      final int producerMaxRate,
                                      final int consumerMaxRate,
                                      final boolean blockOnNonPersistentSend,
                                      final boolean blockOnPersistentSend) throws HornetQException
   {
      synchronized (createSessionLock)
      {
         String name = UUIDGenerator.getInstance().generateSimpleStringUUID().toString();

         boolean retry = false;
         do
         {
            Version clientVersion = VersionLoader.getVersion();

            RemotingConnection theConnection = null;

            Lock lock = null;

            try
            {
               Channel channel1;

               synchronized (failoverLock)
               {
                  theConnection = getConnectionWithRetry(reconnectAttempts);

                  if (theConnection == null)
                  {
                     if (exitLoop)
                     {
                        return null;
                     }
                     // This can happen if the connection manager gets exitLoop - e.g. the server gets shut down

                     throw new HornetQException(HornetQException.NOT_CONNECTED,
                                                "Unable to connect to server using configuration " + connectorConfig);

                  }

                  channel1 = theConnection.getChannel(1, -1);

                  // Lock it - this must be done while the failoverLock is held
                  channel1.getLock().lock();

                  lock = channel1.getLock();
               } // We can now release the failoverLock

               // We now set a flag saying createSession is executing
               synchronized (exitLock)
               {
                  inCreateSession = true;
               }

               long sessionChannelID = theConnection.generateChannelID();

               Packet request = new CreateSessionMessage(name,
                                                         sessionChannelID,
                                                         clientVersion.getIncrementingVersion(),
                                                         username,
                                                         password,
                                                         minLargeMessageSize,
                                                         xa,
                                                         autoCommitSends,
                                                         autoCommitAcks,
                                                         preAcknowledge,
                                                         confWindowSize);

               Packet pResponse;
               try
               {
                  pResponse = channel1.sendBlocking(request);
               }
               catch (HornetQException e)
               {
                  if (e.getCode() == HornetQException.UNBLOCKED)
                  {
                     // This means the thread was blocked on create session and failover unblocked it
                     // so failover could occur

                     retry = true;

                     continue;
                  }
                  else
                  {
                     throw e;
                  }
               }

               CreateSessionResponseMessage response = (CreateSessionResponseMessage)pResponse;
               
               Channel sessionChannel = theConnection.getChannel(sessionChannelID,
                                                              confWindowSize);

               ClientSessionInternal session = new ClientSessionImpl(this,
                                                                     name,
                                                                     username,
                                                                     password,
                                                                     xa,
                                                                     autoCommitSends,
                                                                     autoCommitAcks,
                                                                     preAcknowledge,
                                                                     blockOnAcknowledge,
                                                                     autoGroup,
                                                                     ackBatchSize,
                                                                     consumerWindowSize,
                                                                     consumerMaxRate,
                                                                     confWindowSize,
                                                                     producerWindowSize,
                                                                     producerMaxRate,
                                                                     blockOnNonPersistentSend,
                                                                     blockOnPersistentSend,
                                                                     cacheLargeMessageClient,
                                                                     minLargeMessageSize,
                                                                     theConnection,
                                                                     response.getServerVersion(),
                                                                     sessionChannel,
                                                                     orderedExecutorFactory.getExecutor());

               sessions.add(session);

               ChannelHandler handler = new ClientSessionPacketHandler(session, sessionChannel);

               sessionChannel.setHandler(handler);

               return new DelegatingSession(session);
            }
            catch (Throwable t)
            {
               if (lock != null)
               {
                  lock.unlock();

                  lock = null;
               }

               if (t instanceof HornetQException)
               {
                  throw (HornetQException)t;
               }
               else
               {
                  HornetQException me = new HornetQException(HornetQException.INTERNAL_ERROR,
                                                             "Failed to create session");

                  me.initCause(t);

                  throw me;
               }
            }
            finally
            {
               if (lock != null)
               {
                  lock.unlock();
               }

               // Execution has finished so notify any failover thread that may be waiting for us to be done
               synchronized (exitLock)
               {
                  inCreateSession = false;

                  exitLock.notify();
               }
            }
         }
         while (retry);
      }

      // Should never get here
      throw new IllegalStateException("Oh my God it's full of stars!");
   }

   // Must be synchronized to prevent it happening concurrently with failover which can lead to
   // inconsistencies
   public void removeSession(final ClientSessionInternal session)
   {
      // TODO - can we simplify this locking?
      synchronized (createSessionLock)
      {
         synchronized (failoverLock)
         {
            sessions.remove(session);

            checkCloseConnection();
         }
      }
   }

   public synchronized int numConnections()
   {
      return connection != null ? 1 : 0;
   }

   public int numSessions()
   {
      return sessions.size();
   }

   public void addFailureListener(final SessionFailureListener listener)
   {
      listeners.add(listener);
   }

   public boolean removeFailureListener(final SessionFailureListener listener)
   {
      return listeners.remove(listener);
   }

   public void causeExit()
   {
      exitLoop = true;
   }

   // Public
   // ---------------------------------------------------------------------------------------

   private volatile boolean stopPingingAfterOne;

   public void stopPingingAfterOne()
   {
      this.stopPingingAfterOne = true;
   }

   // Protected
   // ------------------------------------------------------------------------------------

   // Package Private
   // ------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void handleConnectionFailure(final Object connectionID, final HornetQException me)
   {
      failoverOrReconnect(connectionID, me);
   }

   private void failoverOrReconnect(final Object connectionID, final HornetQException me)
   {  
      Set<ClientSessionInternal> sessionsToClose = null;
      
      synchronized (failoverLock)
      {
         if (connection == null || connection.getID() != connectionID)
         {
            // We already failed over/reconnected - probably the first failure came in, all the connections were failed
            // over then a async connection exception or disconnect
            // came in for one of the already exitLoop connections, so we return true - we don't want to call the
            // listeners again
            return;
         }
         
         //We call before reconnection occurs to give the user a chance to do cleanup, like cancel messages
         callFailureListeners(me, false);

         // Now get locks on all channel 1s, whilst holding the failoverLock - this makes sure
         // There are either no threads executing in createSession, or one is blocking on a createSession
         // result.

         // Then interrupt the channel 1 that is blocking (could just interrupt them all)

         // Then release all channel 1 locks - this allows the createSession to exit the monitor

         // Then get all channel 1 locks again - this ensures the any createSession thread has executed the section and
         // returned all its connections to the connection manager (the code to return connections to connection manager
         // must be inside the lock

         // Then perform failover

         // Then release failoverLock

         // The other side of the bargain - during createSession:
         // The calling thread must get the failoverLock and get its' connections when this is locked.
         // While this is still locked it must then get the channel1 lock
         // It can then release the failoverLock
         // It should catch HornetQException.INTERRUPTED in the call to channel.sendBlocking
         // It should then return its connections, with channel 1 lock still held
         // It can then release the channel 1 lock, and retry (which will cause locking on failoverLock
         // until failover is complete

         boolean serverShutdown = me.getCode() == HornetQException.DISCONNECTED;

         // We will try to failover if there is a backup connector factory, but we don't do this if the server
         // has been shutdown cleanly unless failoverOnServerShutdown is true
         boolean attemptFailover = backupConnectorFactory != null && (failoverOnServerShutdown || !serverShutdown);

         boolean attemptReconnect;

         if (attemptFailover)
         {
            attemptReconnect = false;
         }
         else
         {
            attemptReconnect = reconnectAttempts != 0;
         }
         
         if (attemptFailover || attemptReconnect)
         {
            lockChannel1();

            final boolean needToInterrupt;

            synchronized (exitLock)
            {
               needToInterrupt = inCreateSession;
            }

            unlockChannel1();

            if (needToInterrupt)
            {
               // Forcing return all channels won't guarantee that any blocked thread will return immediately
               // So we need to wait for it
               forceReturnChannel1();

               // Now we need to make sure that the thread has actually exited and returned it's connections
               // before failover occurs

               synchronized (exitLock)
               {
                  while (inCreateSession)
                  {
                     try
                     {
                        exitLock.wait(5000);
                     }
                     catch (InterruptedException e)
                     {
                     }
                  }
               }
            }

            // Now we absolutely know that no threads are executing in or blocked in createSession, and no
            // more will execute it until failover is complete

            // So.. do failover / reconnection

            RemotingConnection oldConnection = connection;

            connection = null;

            try
            {
               connector.close();
            }
            catch (Exception ignore)
            {
            }

            connector = null;

            if (attemptFailover)
            {
               // Now try failing over to backup

               connectorFactory = backupConnectorFactory;

               transportParams = backupTransportParams;

               backupConnectorFactory = null;

               backupTransportParams = null;

               reconnectSessions(oldConnection, reconnectAttempts == -1 ? -1 : reconnectAttempts + 1);
            }
            else
            {
               reconnectSessions(oldConnection, reconnectAttempts);
            }

            oldConnection.destroy();
         }
         else
         {
            connection.destroy();

            connection = null;                       
         }      
                          
         callFailureListeners(me, true);
         
         if (connection == null)
         {
            sessionsToClose = new HashSet<ClientSessionInternal>(sessions);
         }
      }
      
      //This needs to be outside the failover lock to prevent deadlock
      if (sessionsToClose != null)
      {
         // If connection is null it means we didn't succeed in failing over or reconnecting
         // so we close all the sessions, so they will throw exceptions when attempted to be used
         
         for (ClientSessionInternal session: sessionsToClose)
         {
            try
            {
               session.cleanUp();
            }
            catch (Exception e)
            {
               log.error("Failed to cleanup session");
            }
         }
      }
   }

   private void callFailureListeners(final HornetQException me, final boolean afterReconnect)
   {
      final List<SessionFailureListener> listenersClone = new ArrayList<SessionFailureListener>(listeners);

      for (final SessionFailureListener listener : listenersClone)
      {
         try
         {
            if (afterReconnect)
            {
               listener.connectionFailed(me);
            }
            else
            {
               listener.beforeReconnect(me);
            }
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            log.error("Failed to execute failure listener", t);
         }
      }
   }

   /*
    * Re-attach sessions all pre-existing sessions to the new remoting connection
    */
   private void reconnectSessions(final RemotingConnection oldConnection, final int reconnectAttempts)
   {        
      RemotingConnection backupConnection = getConnectionWithRetry(reconnectAttempts);
      
      if (backupConnection == null)
      {
         log.warn("Failed to connect to server.");

         return;
      }
      
      List<FailureListener> oldListeners = oldConnection.getFailureListeners();
      
      List<FailureListener> newListeners = new ArrayList<FailureListener>(backupConnection.getFailureListeners());

      for (FailureListener listener : oldListeners)
      {
         // Add all apart from the first one which is the old DelegatingFailureListener

         if (listener instanceof DelegatingFailureListener == false)
         {
            newListeners.add(listener);
         }
      }

      backupConnection.setFailureListeners(newListeners);

      for (ClientSessionInternal session : sessions)
      {
         session.handleFailover(backupConnection);
      }
   }

   private RemotingConnection getConnectionWithRetry(final int reconnectAttempts)
   {
      long interval = retryInterval;

      int count = 0;

      while (true)
      {
         if (exitLoop)
         {
            return null;
         }

         RemotingConnection theConnection = getConnection();

         if (theConnection == null)
         {
            // Failed to get connection

            if (reconnectAttempts != 0)
            {
               count++;

               if (reconnectAttempts != -1 && count == reconnectAttempts)
               {
                  log.warn("Tried " + reconnectAttempts + " times to connect. Now giving up.");

                  return null;
               }

               try
               {
                  Thread.sleep(interval);
               }
               catch (InterruptedException ignore)
               {
               }
               
               // Exponential back-off
               long newInterval = (long)((double)interval * retryIntervalMultiplier);

               if (newInterval > maxRetryInterval)
               {
                  newInterval = maxRetryInterval;
               }

               interval = newInterval;
            }
            else
            {
               return null;
            }
         }
         else
         {
            return theConnection;
         }
      }
   }

   private void checkCloseConnection()
   {
      if (connection != null && sessions.size() == 0)
      {
         if (pingerFuture != null)
         {
            pingRunnable.cancel();

            pingerFuture.cancel(false);

            pingRunnable = null;

            pingerFuture = null;
         }

         try
         {
            connection.destroy();
         }
         catch (Throwable ignore)
         {
         }

         connection = null;

         try
         {
            if (connector != null)
            {
               connector.close();
            }
         }
         catch (Throwable ignore)
         {
         }

         connector = null;
      }
   }

   public RemotingConnection getConnection()
   {
      if (connection == null)
      {
         Connection tc = null;

         try
         {
            if (connector == null)
            {
               DelegatingBufferHandler handler = new DelegatingBufferHandler();

               connector = connectorFactory.createConnector(transportParams,
                                                            handler,
                                                            this,
                                                            threadPool,
                                                            scheduledThreadPool);

               if (connector != null)
               {
                  connector.start();
               }
            }

            if (connector != null)
            {
               tc = connector.createConnection();

               if (tc == null)
               {
                  try
                  {
                     connector.close();
                  }
                  catch (Throwable t)
                  {
                  }

                  connector = null;
               }
            }
         }
         catch (Exception e)
         {
            // Sanity catch for badly behaved remoting plugins

            log.warn("connector.create or connectorFactory.createConnector should never throw an exception, implementation is badly behaved, but we'll deal with it anyway.",
                     e);

            if (tc != null)
            {
               try
               {
                  tc.close();
               }
               catch (Throwable t)
               {
               }
            }

            if (connector != null)
            {
               try
               {
                  connector.close();
               }
               catch (Throwable t)
               {
               }
            }

            tc = null;

            connector = null;
         }

         if (tc == null)
         {
            return connection;
         }

         connection = new RemotingConnectionImpl(tc, callTimeout, interceptors);

         connection.addFailureListener(new DelegatingFailureListener(connection.getID()));

         connection.getChannel(0, -1).setHandler(new Channel0Handler(connection));

         if (clientFailureCheckPeriod != -1)
         {
            if (pingerFuture == null)
            {
               pingRunnable = new PingRunnable();

               pingerFuture = scheduledThreadPool.scheduleWithFixedDelay(new ActualScheduled(pingRunnable),
                                                                         0,
                                                                         clientFailureCheckPeriod,
                                                                         TimeUnit.MILLISECONDS);
            }
            // send a ping every time we create a new remoting connection
            // to set up its TTL on the server side
            else
            {
               pingRunnable.run();
            }
         }
      }

      return connection;
   }

   private ConnectorFactory instantiateConnectorFactory(final String connectorFactoryClassName)
   {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      try
      {
         Class<?> clazz = loader.loadClass(connectorFactoryClassName);
         return (ConnectorFactory)clazz.newInstance();
      }
      catch (Exception e)
      {
         throw new IllegalArgumentException("Error instantiating connector factory \"" + connectorFactoryClassName +
                                            "\"", e);
      }
   }

   private void lockChannel1()
   {
      Channel channel1 = connection.getChannel(1, -1);

      channel1.getLock().lock();
   }

   private void unlockChannel1()
   {
      Channel channel1 = connection.getChannel(1, -1);

      channel1.getLock().unlock();
   }

   private void forceReturnChannel1()
   {
      Channel channel1 = connection.getChannel(1, -1);

      channel1.returnBlocking();
   }

   private void checkTransportKeys(final ConnectorFactory factory, final Map<String, Object> params)
   {
      if (params != null)
      {
         Set<String> invalid = ConfigurationHelper.checkKeys(factory.getAllowableProperties(), params.keySet());

         if (!invalid.isEmpty())
         {
            String msg = ConfigurationHelper.stringSetToCommaListString("The following keys are invalid for configuring a connector: ",
                                                                        invalid);

            throw new IllegalStateException(msg);

         }
      }
   }

   private class Channel0Handler implements ChannelHandler
   {
      private final RemotingConnection conn;

      private Channel0Handler(final RemotingConnection conn)
      {
         this.conn = conn;
      }

      public void handlePacket(final Packet packet)
      {
         final byte type = packet.getType();

         if (type == PacketImpl.DISCONNECT)
         {
            threadPool.execute(new Runnable()
            {
               // Must be executed on new thread since cannot block the netty thread for a long time and fail can
               // cause reconnect loop
               public void run()
               {
                  conn.fail(new HornetQException(HornetQException.DISCONNECTED,
                                                 "The connection was disconnected because of server shutdown"));
               }
            });
         }
      }
   }

   private class DelegatingBufferHandler extends AbstractBufferHandler
   {
      public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
      {
         RemotingConnection theConn = connection;
         
         if (theConn != null && connectionID == theConn.getID())
         {
            theConn.bufferReceived(connectionID, buffer);
         }
      }
   }

   private class DelegatingFailureListener implements FailureListener
   {
      private final Object connectionID;

      DelegatingFailureListener(final Object connectionID)
      {
         this.connectionID = connectionID;
      }

      public void connectionFailed(final HornetQException me)
      {
         handleConnectionFailure(connectionID, me);
      }
   }

   private static final class ActualScheduled implements Runnable
   {
      private final WeakReference<PingRunnable> pingRunnable;

      ActualScheduled(final PingRunnable runnable)
      {
         this.pingRunnable = new WeakReference<PingRunnable>(runnable);
      }

      public void run()
      {
         PingRunnable runnable = pingRunnable.get();

         if (runnable != null)
         {
            runnable.run();
         }
      }

   }

   private final class PingRunnable implements Runnable
   {
      private boolean cancelled;

      private boolean first;

      private long lastCheck = System.currentTimeMillis();

      public synchronized void run()
      {
         if (cancelled || (stopPingingAfterOne && !first))
         {
            return;
         }

         first = false;

         long now = System.currentTimeMillis();

         if (clientFailureCheckPeriod != -1 && now >= lastCheck + clientFailureCheckPeriod)
         {
            if (!connection.checkDataReceived())
            {
               final HornetQException me = new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                                                                "Did not receive data from server for " + connection.getTransportConnection());

               threadPool.execute(new Runnable()
               {
                  // Must be executed on different thread
                  public void run()
                  {
                     connection.fail(me);
                  }
               });

               return;
            }
            else
            {
               lastCheck = now;
            }
         }

         // Send a ping

         Ping ping = new Ping(connectionTTL);

         Channel channel0 = connection.getChannel(0, -1);

         channel0.send(ping);
      }

      public synchronized void cancel()
      {
         cancelled = true;
      }
   }

}
