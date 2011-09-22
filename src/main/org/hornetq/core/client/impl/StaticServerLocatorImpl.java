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
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.utils.ConfigurationHelper;

/**
 * A StaticServerLocatorImpl, was derived from ServerLocatorImpl
 *
 * @author Tim Fox
 * @author <a href="tm.igarashi@gmail.com">Tomohisa Igarashi</a>
 */
public class StaticServerLocatorImpl extends AbstractServerLocator
{
   private static final Logger log = Logger.getLogger(StaticServerLocatorImpl.class);

   private StaticConnector staticConnector = new StaticConnector();

   private final Exception e = new Exception();
   
   @Override
   protected synchronized void initialiseInternal() throws Exception
   {
      /* Nothing special for this class */
   }

   public StaticServerLocatorImpl(final boolean useHA,
                                  final DiscoveryGroupConfiguration discoveryGroupConfiguration)
   {
      super(useHA, discoveryGroupConfiguration);
      
      Map<String,Object> params = discoveryGroupConfiguration.getParams();
      List<TransportConfiguration> initialConnectors = (List<TransportConfiguration>)params.get(DiscoveryGroupConstants.STATIC_CONNECTORS_LIST_NAME);
      setInitialConnectors(initialConnectors.toArray(new TransportConfiguration[0]));
                                            
      e.fillInStackTrace();
   }

   public ClientSessionFactoryInternal connect() throws Exception
   {
      ClientSessionFactoryInternal sf;

      sf = (ClientSessionFactoryInternal)staticConnector.connect();

      addFactory(sf);
      return sf;
   }

   @Override
   protected void waitInitialDiscovery()
   {
      /* Nothing to do for this class */
   }
   
   @Override
   protected void doCloseInternal()
   {
      staticConnector.disconnect();
   }

   class StaticConnector implements Serializable
   {
      private static final long serialVersionUID = 6772279632415242634l;

      private List<Connector> connectors;

      public ClientSessionFactory connect() throws HornetQException
      {
         if (isClosed())
         {
            throw new IllegalStateException("Cannot create session factory, server locator is closed (maybe it has been garbage collected)");
         }

         try
         {
            initialiseInternal();
         }
         catch (Exception e)
         {
            throw new HornetQException(HornetQException.INTERNAL_ERROR, "Failed to initialise session factory", e);
         }

         ClientSessionFactory csf = null;

         createConnectors();

         try
         {

            int retryNumber = 0;
            while (csf == null && !isClosed())
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

               if (!isClosed())
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

         if (csf == null && !isClosed())
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

      class Connector
      {
         private TransportConfiguration initialConnector;

         private volatile ClientSessionFactoryInternal factory;

         private boolean interrupted = false;

         private Exception e;

         public Connector(TransportConfiguration initialConnector, ClientSessionFactoryInternal factory)
         {
            this.initialConnector = initialConnector;
            this.factory = factory;
         }

         public ClientSessionFactory tryConnect() throws HornetQException
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
                  factory.connect(1, false);
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
            interrupted = true;

            if (factory != null)
            {
               factory.causeExit();
               factory.cleanup();
               factory = null;
            }
         }

         /* (non-Javadoc)
          * @see java.lang.Object#toString()
          */
         @Override
         public String toString()
         {
            return "Connector [initialConnector=" + initialConnector + "]";
         }

      }
   }
}
