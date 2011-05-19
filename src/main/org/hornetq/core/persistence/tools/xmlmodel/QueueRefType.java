package org.hornetq.core.persistence.tools.xmlmodel;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for queueRefType complex type.
 * <p/>
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p/>
 * <pre>
 * &lt;complexType name="queueRefType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;attribute name="id" type="{http://www.w3.org/2001/XMLSchema}long" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "queueRefType", namespace = "urn:hornetq")
public class QueueRefType {

   @XmlAttribute
   protected Long id;

   public QueueRefType() {
   }

   public QueueRefType(Long id) {
      this.id = id;
   }

   /**
    * Gets the value of the id property.
    *
    * @return possible object is
    *         {@link Long }
    */
   public Long getId() {
      return id;
   }

   /**
    * Sets the value of the id property.
    *
    * @param value allowed object is
    *              {@link Long }
    */
   public void setId(Long value) {
      this.id = value;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      QueueRefType that = (QueueRefType) o;

      if (!id.equals(that.id)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return id.hashCode();
   }
}
