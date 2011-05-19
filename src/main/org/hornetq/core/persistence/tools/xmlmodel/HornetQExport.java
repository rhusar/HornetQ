package org.hornetq.core.persistence.tools.xmlmodel;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="queues" type="{urn:hornetq}bindings-journalType"/>
 *         &lt;element name="messages" type="{urn:hornetq}messages-journalType"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "queues",
    "messages"
})
@XmlRootElement(name = "hq-journal-export", namespace = "urn:hornetq")
public class HornetQExport {

    @XmlElement(namespace = "urn:hornetq", required = true)
    protected BindingsJournalType queues;
    @XmlElement(namespace = "urn:hornetq", required = true)
    protected MessagesExportType messages;

    /**
     * Gets the value of the queues property.
     * 
     * @return
     *     possible object is
     *     {@link BindingsJournalType }
     *     
     */
    public BindingsJournalType getQueues() {
        return queues;
    }

    /**
     * Sets the value of the queues property.
     * 
     * @param value
     *     allowed object is
     *     {@link BindingsJournalType }
     *     
     */
    public void setQueues(BindingsJournalType value) {
        this.queues = value;
    }

    /**
     * Gets the value of the messages property.
     * 
     * @return
     *     possible object is
     *     {@link MessagesExportType }
     *     
     */
    public MessagesExportType getMessages() {
        return messages;
    }

    /**
     * Sets the value of the messages property.
     * 
     * @param value
     *     allowed object is
     *     {@link MessagesExportType }
     *     
     */
    public void setMessages(MessagesExportType value) {
        this.messages = value;
    }

}
