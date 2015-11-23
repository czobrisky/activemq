package org.apache.activemq.broker.virtual;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.spring.ConsumerBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtualTopicInterceptorTest extends EmbeddedBrokerTestSupport{

	private static final Logger LOG = LoggerFactory.getLogger(VirtualTopicInterceptorTest.class);
	protected int total = 10;
	protected Connection connection;	
	
	public static Test suite() {
        return suite(VirtualTopicInterceptorTest.class);
    }
	
	public void testVirtualTopicRouting() throws Exception{
		ConsumerBean messageList1 = new ConsumerBean();
	    ConsumerBean messageList2 = new ConsumerBean();	    
		testVirtualTopicRouting(messageList1, messageList2);
	}
	
    protected Destination getConsumer1Destination() {
        return new ActiveMQQueue("q.private.vt.testing.test.virtual.topic");
    }

    protected Destination getConsumer2Destination() {
        return new ActiveMQQueue("q.private.>");
    }     
    
    protected Destination getProducerDestination() {
        return new ActiveMQTopic("test.virtual.topic");
    }
    
    
    private void testVirtualTopicRouting(ConsumerBean messageList1, ConsumerBean messageList2) throws Exception {
    	if (connection == null) {
            connection = createConnection();
        }
        connection.start();
        
        LOG.info("validate no other messages on queues");        
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                
            Destination destination1 = getConsumer1Destination();
            Destination destination2 = getConsumer2Destination();
            
            MessageConsumer c1 = session.createConsumer(destination1, null);
            MessageConsumer c2 = session.createConsumer(destination2, null);
            
            c1.setMessageListener(messageList1);
            c2.setMessageListener(messageList2);
            
            LOG.info("send one simple message that should go to both consumers");
            MessageProducer producer = session.createProducer(getProducerDestination());
            assertNotNull(producer);
            
            producer.send(session.createTextMessage("Last Message"));
            //messageList1 should have correct amount - q.private.consumer.1.vt.TEST
            messageList1.assertMessagesArrived(1);
            //messageList2 should have 1 if broken - q.private.>
            messageList2.assertMessagesArrived(1);
            
        } catch (JMSException e) {
            e.printStackTrace();
            fail("unexpeced ex while waiting for last messages: " + e);
        }
    }
    
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }
    
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);

        VirtualTopic virtualTopic = new VirtualTopic();
        virtualTopic.setName(">");
        virtualTopic.setPrefix("q.private.vt.*.");
        VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
        interceptor.setVirtualDestinations(new VirtualDestination[]{virtualTopic});        
        broker.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});
        return broker;
    }
}
