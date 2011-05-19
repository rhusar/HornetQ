package org.hornetq.core.persistence.tools.xmlmodel;

import javax.xml.bind.annotation.XmlRegistry;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the org.hornetq.core.journal.export package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {


    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: org.hornetq.core.journal.export
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link BindType }
     * 
     */
    public BindType createBindType() {
        return new BindType();
    }

    /**
     * Create an instance of {@link HornetQExport }
     * 
     */
    public HornetQExport createHqJournalExport() {
        return new HornetQExport();
    }

    /**
     * Create an instance of {@link AckType }
     * 
     */
    public AckType createAckType() {
        return new AckType();
    }

    /**
     * Create an instance of {@link PropertiesType }
     * 
     */
    public PropertiesType createPropertiesType() {
        return new PropertiesType();
    }

    /**
     * Create an instance of {@link PropertyType }
     * 
     */
    public PropertyType createPropertyType() {
        return new PropertyType();
    }

    /**
     * Create an instance of {@link MessageType }
     * 
     */
    public MessageType createMessageType() {
        return new MessageType();
    }

    /**
     * Create an instance of {@link QueueRefType }
     * 
     */
    public QueueRefType createQueueRefType() {
        return new QueueRefType();
    }

    /**
     * Create an instance of {@link QueueType }
     * 
     */
    public QueueType createQueueType() {
        return new QueueType();
    }

    /**
     * Create an instance of {@link BindingsType }
     * 
     */
    public BindingsType createBindingsType() {
        return new BindingsType();
    }

    /**
     * Create an instance of {@link BindingsJournalType }
     * 
     */
    public BindingsJournalType createBindingsJournalType() {
        return new BindingsJournalType();
    }

    /**
     * Create an instance of {@link MessagesExportType }
     * 
     */
    public MessagesExportType createMessagesJournalType() {
        return new MessagesExportType();
    }

}
