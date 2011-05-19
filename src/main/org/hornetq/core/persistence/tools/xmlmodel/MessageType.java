package org.hornetq.core.persistence.tools.xmlmodel;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.server.ServerMessage;


/**
 * <p>Java class for messageType complex type.
 * <p/>
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p/>
 * <pre>
 * &lt;complexType name="messageType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="address" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="type" type="{http://www.w3.org/2001/XMLSchema}byte"/>
 *         &lt;element name="durable" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *         &lt;element name="expiration" type="{http://www.w3.org/2001/XMLSchema}long"/>
 *         &lt;element name="timestamp" type="{http://www.w3.org/2001/XMLSchema}long"/>
 *         &lt;element name="properties" type="{urn:hornetq}propertiesType"/>
 *         &lt;element name="priority" type="{http://www.w3.org/2001/XMLSchema}byte"/>
 *         &lt;element name="payload" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="userId" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="bindings" type="{urn:hornetq}bindType"/>
 *         &lt;element name="acks" type="{urn:hornetq}ackType"/>
 *       &lt;/sequence>
 *       &lt;attribute name="id" type="{http://www.w3.org/2001/XMLSchema}long" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "messageType", namespace = "urn:hornetq", propOrder = {
      "address",
      "type",
      "durable",
      "expiration",
      "timestamp",
      "properties",
      "priority",
      "payload",
      "userId",
      "bindings",
      "acks"
})
public class MessageType {

   @XmlElement(namespace = "urn:hornetq", required = true)
   protected String address;
   @XmlElement(namespace = "urn:hornetq")
   protected byte type;
   @XmlElement(namespace = "urn:hornetq")
   protected boolean durable;
   @XmlElement(namespace = "urn:hornetq")
   protected long expiration;
   @XmlElement(namespace = "urn:hornetq")
   protected long timestamp;
   @XmlElement(namespace = "urn:hornetq", required = true)
   protected PropertiesType properties;
   @XmlElement(namespace = "urn:hornetq")
   protected byte priority;
   @XmlElement(namespace = "urn:hornetq", required = true)
   protected String payload;
   @XmlElement(namespace = "urn:hornetq", required = true)
   protected String userId;
   @XmlElement(namespace = "urn:hornetq", required = true)
   protected BindType bindings = new BindType();
   @XmlElement(namespace = "urn:hornetq", required = true)
   protected AckType acks = new AckType();
   @XmlAttribute
   protected Long id;

   public MessageType(ServerMessage msg) {
      setId(msg.getMessageID());

      setAddress(msg.getAddress() != null ? msg.getAddress().toString() : "");

      setType(msg.getType());
      setDurable(msg.isDurable());
      setExpiration(msg.getExpiration());
      setTimestamp(msg.getTimestamp());

      PropertiesType properties = new PropertiesType();
      for (SimpleString propName : msg.getPropertyNames()) {
         PropertyType propertyType = new PropertyType();
         propertyType.setKey(propName.toString());
         propertyType.setValue(msg.getSimpleStringProperty(propName).toString());
         properties.getProperty().add(propertyType);
      }
      setProperties(properties);
      setPriority(msg.getPriority());
      if (msg.getUserID() != null)
      {
         setUserId(msg.getUserID().toString());
      }
      else
      {
         setUserId("");
      }
   }

   /**
    * Ctor for equals.
    *
    * @param id the messageID
    */
   public MessageType(long id) {
      setId(id);
   }

   /**
    * Default Ctor.
    */
   public MessageType() {
   }

   /**
    * Gets the value of the address property.
    *
    * @return possible object is
    *         {@link String }
    */
   public String getAddress() {
      return address;
   }

   /**
    * Sets the value of the address property.
    *
    * @param value allowed object is
    *              {@link String }
    */
   public void setAddress(String value) {
      this.address = value;
   }

   /**
    * Gets the value of the type property.
    */
   public byte getType() {
      return type;
   }

   /**
    * Sets the value of the type property.
    */
   public void setType(byte value) {
      this.type = value;
   }

   /**
    * Gets the value of the durable property.
    */
   public boolean isDurable() {
      return durable;
   }

   /**
    * Sets the value of the durable property.
    */
   public void setDurable(boolean value) {
      this.durable = value;
   }

   /**
    * Gets the value of the expiration property.
    */
   public long getExpiration() {
      return expiration;
   }

   /**
    * Sets the value of the expiration property.
    */
   public void setExpiration(long value) {
      this.expiration = value;
   }

   /**
    * Gets the value of the timestamp property.
    */
   public long getTimestamp() {
      return timestamp;
   }

   /**
    * Sets the value of the timestamp property.
    */
   public void setTimestamp(long value) {
      this.timestamp = value;
   }

   /**
    * Gets the value of the properties property.
    *
    * @return possible object is
    *         {@link PropertiesType }
    */
   public PropertiesType getProperties() {
      return properties;
   }

   /**
    * Sets the value of the properties property.
    *
    * @param value allowed object is
    *              {@link PropertiesType }
    */
   public void setProperties(PropertiesType value) {
      this.properties = value;
   }

   /**
    * Gets the value of the priority property.
    */
   public byte getPriority() {
      return priority;
   }

   /**
    * Sets the value of the priority property.
    */
   public void setPriority(byte value) {
      this.priority = value;
   }

   /**
    * Gets the value of the payload property.
    *
    * @return possible object is
    *         {@link String }
    */
   public String getPayload() {
      return payload;
   }

   /**
    * Sets the value of the payload property.
    *
    * @param value allowed object is
    *              {@link String }
    */
   public void setPayload(String value) {
      this.payload = value;
   }

   /**
    * Gets the value of the userId property.
    *
    * @return possible object is
    *         {@link String }
    */
   public String getUserId() {
      return userId;
   }

   /**
    * Sets the value of the userId property.
    *
    * @param value allowed object is
    *              {@link String }
    */
   public void setUserId(String value) {
      this.userId = value;
   }

   /**
    * Gets the value of the bindings property.
    *
    * @return possible object is
    *         {@link BindType }
    */
   public BindType getBindings() {
      return bindings;
   }

   /**
    * Sets the value of the bindings property.
    *
    * @param value allowed object is
    *              {@link BindType }
    */
   public void setBindings(BindType value) {
      this.bindings = value;
   }

   /**
    * Gets the value of the acks property.
    *
    * @return possible object is
    *         {@link AckType }
    */
   public AckType getAcks() {
      return acks;
   }

   /**
    * Sets the value of the acks property.
    *
    * @param value allowed object is
    *              {@link AckType }
    */
   public void setAcks(AckType value) {
      this.acks = value;
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

      MessageType that = (MessageType) o;

      if (!id.equals(that.id)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return id.hashCode();
   }
}
