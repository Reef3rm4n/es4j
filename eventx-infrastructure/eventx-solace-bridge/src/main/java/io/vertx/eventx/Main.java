//package io.vertx.eventx;
//
//import com.solacesystems.jcsmp.Destination;
//import com.solacesystems.jms.SolJmsUtility;
//
//import javax.jms.MessageConsumer;
//import javax.jms.MessageProducer;
//import javax.jms.Session;
//
//public class Main {
//  public static void main(String[] args) throws Exception {
//    final var connection = SolJmsUtility.createConnectionFactory();
//    final var con = connection.createConnection();
//    final var session = con.createSession(true, Session.SESSION_TRANSACTED);
//    final var consumer = session.createConsumer(null);
//    final var producer = session.createProducer(null);
//    domainLogic(session, consumer,producer);
//    session.close();
//  }
//
//  public static void domainLogic(Session session, MessageConsumer consumer, MessageProducer producer) {
////    timerTask that manually polls from queue
//    consumer.receive();
//    producer.send();
//    // do database transaction
//    session.commit();
//  }
//
//}
