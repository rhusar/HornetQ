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

package org.hornetq.core.persistence.tools;

/**
 * A ExportData
 *
 * @author clebertsuconic
 *
 *
 */
public class ExportData
{

   public static void main(String args[])
   {
      if (args.length != 2) {
         // todo: maybe just use a hq-config.xml file as a parameter
         System.out.println("Usage Export: java org.hornetq.core.persistence.tools.ExportData <bindings-dir> <journal-dir>");
         System.exit(-1);
      }
      try
      {
         ManageDataTool.exportMessages(args[0], args[1], System.out);
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }
      
   }
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
