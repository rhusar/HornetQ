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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConstants;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.FailureListener;

/**
 * A StaticServerLocatorImpl
 * @author Tim Fox
 */
public class StaticServerLocatorImpl extends AbstractServerLocator
{
   private static final long serialVersionUID = 6473494410188544024L;

   private static final Logger log = Logger.getLogger(StaticServerLocatorImpl.class);

   private final StaticConnector staticConnector = new StaticConnector();

   private final Exception e = new Exception();

   // To be called when there are ServerLocator being finalized.
   // To be used on test assertions
   public static Runnable finalizeCallback = null;

   @Override
   protected void initialiseInternal()
   {
      // Nothing for this ServerLocator
   }

   private StaticServerLocatorImpl(final Topology topology,
                             final boolean useHA,
                             final DiscoveryGroupConfiguration discoveryGroupConfiguration,
                             final TransportConfiguration[] transportConfigs)
   {
      super(topology, useHA, discoveryGroupConfiguration, transportConfigs);

      Map<String, Object> params = discoveryGroupConfiguration.getParams();
      List<TransportConfiguration> initialConnectors =
               (List<TransportConfiguration>)params.get(DiscoveryGroupConstants.STATIC_CONNECTOR_CONFIG_LIST_NAME);
      setStaticTransportConfigurations(initialConnectors.toArray(new TransportConfiguration[0]));

      e.fillInStackTrace();
   }

   /**
    * Create a ServerLocatorImpl using UDP discovery to lookup cluster
    *
    * @param discoveryAddress
    * @param discoveryPort
    */
   public StaticServerLocatorImpl(final boolean useHA, final DiscoveryGroupConfiguration groupConfiguration)
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
   public StaticServerLocatorImpl(final boolean useHA, final TransportConfiguration... transportConfigs)
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
   public StaticServerLocatorImpl(final Topology topology,
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
   public StaticServerLocatorImpl(final Topology topology,
                            final boolean useHA,
                            final TransportConfiguration... transportConfigs)
   {
      this(topology, useHA, null, transportConfigs);
   }

   @Override
   public ClientSessionFactoryInternal connect() throws Exception
   {
      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal)staticConnector.connect();
      addFactory(sf);
      return sf;
   }

   @Override
   protected void waitInitialDiscovery()
   {
      // Nothing for this ServerLocator
   }

   @Override
   protected void doCloseInternal()
   {
      staticConnector.disconnect();
   }

   private final class StaticConnector implements Serializable
   {
      private static final long serialVersionUID = 6772279632415242634l;

      private List<Connector> connectors;

      public ClientSessionFactory connect() throws HornetQException
      {
         if (isClosed())
         {
            throw new IllegalStateException("Cannot create session factory, server locator is closed (maybe it has been garbage collected)");
         }

         initialise();

         ClientSessionFactory csf = null;

         createConnectors();

         try
         {

            int retryNumber = 0;
            while (csf == null && isInitialized())
            {
               retryNumber++;
               for (Connector conn : connectors)
               {
                  if (log.isDebugEnabled())
                  {
                     log.debug(this + "::Submitting connect towards " + conn);
                  }

                  csf = conn.tryConnect();

                  if (csf != null)
                  {
                     csf.getConnection().addFailureListener(new FailureListener()
                     {
                        // Case the node where the cluster connection was connected is gone, we need to restart the
                        // connection
                        public void connectionFailed(HornetQException exception, boolean failedOver)
                        {
                           if (isClusterConnection() && exception.getCode() == HornetQException.DISCONNECTED)
                           {
                              try
                              {
                                 StaticServerLocatorImpl.this.start(getExecutor());
                              }
                              catch (Exception e)
                              {
                                 // There isn't much to be done if this happens here
                                 log.warn(e.getMessage());
                              }
                           }
                        }
                     });

                     if (log.isDebugEnabled())
                     {
                        log.debug("Returning " + csf +
                                  " after " +
                                  retryNumber +
                                  " retries on StaticConnector " +
                                 StaticServerLocatorImpl.this);
                     }

                     return csf;
                  }
               }

               if (getInitialConnectAttempts() >= 0 && retryNumber > getInitialConnectAttempts())
               {
                  break;
               }

               if (isInitialized())
               {
                  Thread.sleep(getRetryInterval());
               }
            }

         }
         catch (Exception e)
         {
            log.warn(e.getMessage(), e);
            throw new HornetQException(HornetQException.NOT_CONNECTED, "Failed to connect to any static connectors", e);
         }

         if (csf == null && isInitialized())
         {
            log.warn("Failed to connecto to any static connector, throwing exception now");
            throw new HornetQException(HornetQException.NOT_CONNECTED, "Failed to connect to any static connectors");
         }
         if (log.isDebugEnabled())
         {
            log.debug("Returning " + csf + " on " + StaticServerLocatorImpl.this);
         }
         return csf;
      }

      private synchronized void createConnectors()
      {
         if (connectors != null)
         {
            for (Connector conn : connectors)
            {
               if (conn != null)
               {
                  conn.disconnect();
               }
            }
         }
         connectors = new ArrayList<Connector>();
         for (TransportConfiguration initialConnector : getStaticTransportConfigurations())
         {
            assertOpen();
            ClientSessionFactoryInternal factory =
                     new ClientSessionFactoryImpl(StaticServerLocatorImpl.this,
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

            factory.disableFinalizeCheck();

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

      @Override
      public void finalize() throws Throwable
      {
         if (getState() != STATE.CLOSED && isFinalizeCheckEnabled())
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

      private class Connector
      {
         private final TransportConfiguration initialConnector;

         private volatile ClientSessionFactoryInternal factory;

         private Exception e;

         Connector(TransportConfiguration initialConnector, ClientSessionFactoryInternal factory)
         {
            this.initialConnector = initialConnector;
            this.factory = factory;
         }

         private ClientSessionFactory tryConnect() throws HornetQException
         {
            if (log.isDebugEnabled())
            {
               log.debug(this + "::Trying to connect to " + factory);
            }
            try
            {
               ClientSessionFactoryInternal factoryToUse = factory;
               if (factoryToUse != null)
               {
                  addToConnecting(factoryToUse);

                  try
                  {
                     factoryToUse.connect(1, false);
                  }
                  finally
                  {
                     removeFromConnecting(factoryToUse);
                  }
               }
               return factoryToUse;
            }
            catch (HornetQException e)
            {
               log.debug(this + "::Exception on establish connector initial connection", e);
               return null;
            }
         }

         public void disconnect()
         {
            if (factory != null)
            {
               factory.causeExit();
               factory.cleanup();
               factory = null;
            }
         }

         @Override
         public String toString()
         {
            return "Connector [initialConnector=" + initialConnector + "]";
         }

      }
   }
}
