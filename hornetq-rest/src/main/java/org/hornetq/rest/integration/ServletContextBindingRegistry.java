package org.hornetq.rest.integration;

import org.hornetq.spi.core.naming.BindingRegistry;

import javax.servlet.ServletContext;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class ServletContextBindingRegistry implements BindingRegistry
{
   private ServletContext servletContext;

   public ServletContextBindingRegistry(ServletContext servletContext)
   {
      this.servletContext = servletContext;
   }

   @Override
   public Object lookup(String name)
   {
      return servletContext.getAttribute(name);
   }

   @Override
   public boolean bind(String name, Object obj)
   {
      servletContext.setAttribute(name, obj);
      return true;
   }

   @Override
   public void unbind(String name)
   {
      servletContext.removeAttribute(name);
   }

   @Override
   public void close()
   {
   }

   @Override
   public Object getContext()
   {
      return servletContext;
   }

   @Override
   public void setContext(Object o)
   {
      servletContext = (ServletContext)o;
   }
}
