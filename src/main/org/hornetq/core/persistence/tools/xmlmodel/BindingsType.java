package org.hornetq.core.persistence.tools.xmlmodel;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for bindingsType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="bindingsType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="queue" type="{urn:hornetq}queueType"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "bindingsType", namespace = "urn:hornetq", propOrder = {
    "queue"
})
public class BindingsType {

    @XmlElement(namespace = "urn:hornetq", required = true)
    protected QueueType queue;

    /**
     * Gets the value of the queue property.
     * 
     * @return
     *     possible object is
     *     {@link QueueType }
     *     
     */
    public QueueType getQueue() {
        return queue;
    }

    /**
     * Sets the value of the queue property.
     * 
     * @param value
     *     allowed object is
     *     {@link QueueType }
     *     
     */
    public void setQueue(QueueType value) {
        this.queue = value;
    }

}
