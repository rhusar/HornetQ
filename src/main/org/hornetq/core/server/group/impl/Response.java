/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.core.server.group.impl;

import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class Response
{
   private final boolean accepted;

   private final Object original;

   private final Object alternative;

   private SimpleString responseType;

   public Response(SimpleString responseType, Object original)
   {
      this(responseType, original, null);
   }

   public Response(SimpleString responseType, Object original, Object alternative)
   {
      this.responseType = responseType;
      this.accepted = alternative == null;
      this.original = original;
      this.alternative = alternative;
   }

   public boolean isAccepted()
   {
      return accepted;
   }

   public Object getOriginal()
   {
      return original;
   }

   public Object getAlternative()
   {
      return alternative;
   }

   public Object getChosen()
   {
      return alternative != null?alternative:original;
   }

   @Override
   public String toString()
   {
      return "accepted = " + accepted + " original = " + original + " alternative = " + alternative;
   }

   public SimpleString getResponseType()
   {
      return responseType;
   }
}
