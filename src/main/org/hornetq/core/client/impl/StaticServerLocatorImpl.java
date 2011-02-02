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

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConstants;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.ConfigurationHelper;

/**
 * A SimpleUDPServerLocatorImpl
 *
 * @author Tim Fox
 * @author <a href="tm.igarashi@gmail.com">Tomohisa Igarashi</a>
 */
public class StaticServerLocatorImpl extends AbstractServerLocator
{
   private static final long serialVersionUID = -1615857864410205260L;

   private static final Logger log = Logger.getLogger(StaticServerLocatorImpl.class);

   private StaticConnector staticConnector = new StaticConnector();

   private volatile boolean closing;
   
   private final Exception e = new Exception();
   
   // To be called when there are ServerLocator being finalized.
   // To be used on test assertions
   public static Runnable finalizeCallback = null;

   private synchronized void initialise() throws Exception
   {
      if (!isReadOnly())
      {
         setThreadPools();

         instantiateLoadBalancingPolicy();

         Map<String,Object> params = getDiscoveryGroupConfiguration().getParams();
         TransportConfiguration[] initialConnectors = (TransportConfiguration[])params.get(DiscoveryGroupConstants.STATIC_CONNECTORS_LIST_NAME);
         setInitialConnectors(initialConnectors);
         
         setReadOnly(true);
      }
   }

   public StaticServerLocatorImpl(final boolean useHA,
                             final DiscoveryGroupConfiguration discoveryGroupConfiguration)
   {
      super(useHA, discoveryGroupConfiguration);
      
      e.fillInStackTrace();
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

      sf = (ClientSessionFactoryInternal)staticConnector.connect();

      addFactory(sf);
      return sf;
   }

   public ClientSessionFactory createSessionFactory(final TransportConfiguration transportConfiguration) throws Exception
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

   public void close()
   {
      if (isClosed())
      {
         return;
      }

      closing = true;

      staticConnector.disconnect();

      super.close();
   }

   public boolean isStaticDirectConnection(TransportConfiguration con)
   {
      for(TransportConfiguration connector : getInitialConnectors())
      {
         if(connector.equals(con))
         {
            return true;
         }
      }
      return false;
   }
   
   class StaticConnector implements Serializable
   {
      private List<Connector> connectors;

      public ClientSessionFactory connect() throws HornetQException
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

         ClientSessionFactory csf = null;

         createConnectors();

         try
         {
            List<Future<ClientSessionFactory>> futures = getThreadPool().invokeAll(connectors);
            for (int i = 0, futuresSize = futures.size(); i < futuresSize; i++)
            {
               Future<ClientSessionFactory> future = futures.get(i);
               try
               {
                  csf = future.get();
                  if (csf != null)
                     break;
               }
               catch (Exception e)
               {
                  log.debug("unable to connect with static connector " + connectors.get(i).initialConnector);
               }
            }
            if (csf == null && !isClosed())
            {
               throw new HornetQException(HornetQException.NOT_CONNECTED, "Failed to connect to any static connectors");
            }
         }
         catch (InterruptedException e)
         {
            throw new HornetQException(HornetQException.NOT_CONNECTED, "Failed to connect to any static connectors", e);
         }

         if (csf == null && !isClosed())
         {
            throw new HornetQException(HornetQException.NOT_CONNECTED, "Failed to connect to any static connectors");
         }
         return csf;
      }

      private synchronized void createConnectors()
      {
         connectors = new ArrayList<Connector>();
         for (TransportConfiguration initialConnector : getInitialConnectors())
         {
            ClientSessionFactoryInternal factory = new ClientSessionFactoryImpl(StaticServerLocatorImpl.this,
                                                                                initialConnector,
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
            connectors.add(new Connector(initialConnector, factory));
         }
      }

      public synchronized void disconnect()
      {
         if (connectors != null)
         {
            for (Connector connector : connectors)
            {
               connector.disconnect();
            }
         }
      }

      public void finalize() throws Throwable
      {
         if (!isClosed() && doFinalizeCheck())
         {
            log.warn("I'm closing a core ServerLocator you left open. Please make sure you close all ServerLocators explicitly " + "before letting them go out of scope! " +
                     System.identityHashCode(this));

            log.warn("The ServerLocator you didn't close was created here:", e);
            
            if (StaticServerLocatorImpl.finalizeCallback != null)
            {
               StaticServerLocatorImpl.finalizeCallback.run();
            }

            close();
         }

         super.finalize();
      }

      class Connector implements Callable<ClientSessionFactory>
      {
         private TransportConfiguration initialConnector;

         private volatile ClientSessionFactoryInternal factory;

         private boolean isConnected = false;

         private boolean interrupted = false;

         private Exception e;

         public Connector(TransportConfiguration initialConnector, ClientSessionFactoryInternal factory)
         {
            this.initialConnector = initialConnector;
            this.factory = factory;
         }

         public ClientSessionFactory call() throws HornetQException
         {
            try
            {
               factory.connect(getReconnectAttempts(), isFailoverOnInitialConnection());
            }
            catch (HornetQException e)
            {
               if (!interrupted)
               {
                  this.e = e;
                  throw e;
               }
               /*if(factory != null)
               {
                  factory.close();
                  factory = null;
               }*/
               return null;
            }
            isConnected = true;
            for (Connector connector : connectors)
            {
               if (!connector.isConnected())
               {
                  connector.disconnect();
               }
            }
            return factory;
         }

         public boolean isConnected()
         {
            return isConnected;
         }

         public void disconnect()
         {
            interrupted = true;

            if (factory != null)
            {
               factory.causeExit();
               factory.close();
               factory = null;
            }
         }
      }
   }
}
