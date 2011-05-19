package org.hornetq.core.persistence.tools.xmlmodel;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for ackType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="ackType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="queue" type="{urn:hornetq}queueRefType" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ackType", namespace = "urn:hornetq", propOrder = {
    "queue"
})
public class AckType {

    @XmlElement(namespace = "urn:hornetq")
    protected List<QueueRefType> queue;

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
     * {@link QueueRefType }
     * 
     * 
     */
    public List<QueueRefType> getQueue() {
        if (queue == null) {
            queue = new ArrayList<QueueRefType>();
        }
        return this.queue;
    }

}
