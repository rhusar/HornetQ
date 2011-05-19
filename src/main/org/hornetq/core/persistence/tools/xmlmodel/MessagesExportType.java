package org.hornetq.core.persistence.tools.xmlmodel;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for messages-journalType complex type.
 * <p/>
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p/>
 * <pre>
 * &lt;complexType name="messages-journalType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="message" type="{urn:hornetq}messageType" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "messages-journalType", namespace = "urn:hornetq", propOrder = {
      "message"
})
public class MessagesExportType {

   @XmlElement(namespace = "urn:hornetq")
   protected List<MessageType> message;

   @XmlTransient
   private BindingsJournalType bindings;

   /**
    * Gets the value of the message property.
    * <p/>
    * <p/>
    * This accessor method returns a reference to the live list,
    * not a snapshot. Therefore any modification you make to the
    * returned list will be present inside the JAXB object.
    * This is why there is not a <CODE>set</CODE> method for the message property.
    * <p/>
    * <p/>
    * For example, to add a new item, do as follows:
    * <pre>
    *    getMessage().add(newItem);
    * </pre>
    * <p/>
    * <p/>
    * <p/>
    * Objects of the following type(s) are allowed in the list
    * {@link MessageType }
    */
   public List<MessageType> getMessage() {
      if (message == null) {
         message = new ArrayList<MessageType>();
      }
      return this.message;
   }

   /**
    * Install a listener for messages on this object. If listener is null, the listener
    * is removed again.
    */
   public void setMessageListener(final Listener listener) {
      message = (listener == null) ? null : new ArrayList<MessageType>() {
         public boolean add(MessageType o) {
            try {
               o.setAllPreviousBindings(getBindings());
               listener.handleMessage(o);
            } catch (Exception e) {
               e.printStackTrace();
               throw new RuntimeException("Could not handle message " + o);
            }
            return false;
         }
      };
   }

   public void setOriginalBindings(final BindingsJournalType bindings) {
      this.bindings = bindings;
   }

   public BindingsJournalType getBindings() {
      return bindings;
   }

   /**
    * This listener is invoked every time a new message is unmarshalled.
    */
   public static interface Listener {
      void handleMessage(MessageType message) throws Exception;
   }
}
