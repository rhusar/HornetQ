package org.hornetq.core.persistence.tools.xmlmodel;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for bindings-journalType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="bindings-journalType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="queue" type="{urn:hornetq}queueType" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "bindings-journalType", namespace = "urn:hornetq", propOrder = {
    "queue"
})
public class BindingsJournalType {

    @XmlElement(namespace = "urn:hornetq")
    protected List<QueueType> queue;

    /**
     * Gets the value of the queue property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the queue property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getQueue().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link QueueType }
     * 
     * 
     */
    public List<QueueType> getQueue() {
        if (queue == null) {
            queue = new ArrayList<QueueType>();
        }
        return this.queue;
    }

}
