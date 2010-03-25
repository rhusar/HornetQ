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

package org.hornetq.jms.persistence;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.utils.BufferHelper;
import org.hornetq.utils.DataConstants;

/**
 * A PersistedDestination
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class PersistedDestination implements EncodingSupport
{


   // Constants -----------------------------------------------------

   public enum Type
   {
      QUEUE,
      TOPIC;

      public int getType()
      {
         return this == QUEUE ? 1 : 2;
      }

      static Type getType(int type)
      {
         return type == 1 ? QUEUE : TOPIC;
      }
   }
   // Attributes ----------------------------------------------------

   private long id;

   private Type type;

   private String name;

   private String jndiBinding;

   private String selector;

   private boolean durable;
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PersistedDestination()
   {
   }

   public PersistedDestination(final Type type, final String name, final String jndiBinding)
   {
      this(type, name, jndiBinding, null, false);
   }

   public PersistedDestination(final Type type, final String name, final String jndiBinding, final String selector, final boolean durable)
   {
      this.type = type;
      this.name = name;
      this.jndiBinding = jndiBinding;
      this.selector = selector;
      this.durable = durable;
   }
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------


   public long getId()
   {
      return id;
   }

   public void setId(final long id)
   {
      this.id = id;
   }

   public String getName()
   {
      return name;
   }

   public String getJndiBinding()
   {
      return jndiBinding;
   }

   public Type getType()
   {
      return type;
   }

   public String getSelector()
   {
      return selector;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public int getEncodeSize()
   {
      return DataConstants.SIZE_INT +
            BufferHelper.sizeOfSimpleString(name) +
            BufferHelper.sizeOfSimpleString(jndiBinding) +
            BufferHelper.sizeOfNullableSimpleString(selector) +
            DataConstants.SIZE_BOOLEAN;
   }

   public void encode(final HornetQBuffer buffer)
   {
      buffer.writeInt(type.getType());
      buffer.writeString(name);
      buffer.writeString(jndiBinding);
      buffer.writeNullableString(selector);
      buffer.writeBoolean(durable);
   }

   public void decode(final HornetQBuffer buffer)
   {
      type = Type.getType(buffer.readInt());
      name = buffer.readString();
      jndiBinding = buffer.readString();
      selector = buffer.readNullableString();
      durable = buffer.readBoolean();
   }
}
