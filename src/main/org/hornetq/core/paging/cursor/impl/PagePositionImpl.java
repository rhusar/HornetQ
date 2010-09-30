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

package org.hornetq.core.paging.cursor.impl;

import org.hornetq.core.paging.cursor.PagePosition;

/**
 * A PagePosition
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class PagePositionImpl implements PagePosition
{
   private long pageNr;

   private long messageNr;

   /** ID used for storage */
   private long recordID;
   
   

   /**
    * @param pageNr
    * @param messageNr
    */
   public PagePositionImpl(long pageNr, long messageNr)
   {
      super();
      this.pageNr = pageNr;
      this.messageNr = messageNr;
   }

   /**
    * @return the recordID
    */
   public long getRecordID()
   {
      return recordID;
   }

   /**
    * @param recordID the recordID to set
    */
   public void setRecordID(long recordID)
   {
      this.recordID = recordID;
   }

   /**
    * @return the pageNr
    */
   public long getPageNr()
   {
      return pageNr;
   }

   /**
    * @return the messageNr
    */
   public long getMessageNr()
   {
      return messageNr;
   }

   /* (non-Javadoc)
    * @see java.lang.Comparable#compareTo(java.lang.Object)
    */
   public int compareTo(PagePosition o)
   {
      if (pageNr > o.getPageNr())
      {
         return 1;
      }
      else if (pageNr < o.getPageNr())
      {
         return -1;
      }
      else if (recordID > o.getRecordID())
      {
         return 1;
      }
      else if (recordID < o.getRecordID())
      {
         return -1;
      }
      else
      {
         return 0;
      }
   }
   
   public boolean isNextSequenceOf(PagePosition pos)
   {
      return this.pageNr == pos.getPageNr() && this.getRecordID() - pos.getRecordID() == 1;
   }

}
