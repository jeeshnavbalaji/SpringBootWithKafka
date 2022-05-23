package com.demo.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.swing.text.Document;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

@Service
public class Consumer {

    private final Logger logger = LoggerFactory.getLogger(Producer.class);

    //    receive all the messages sent to a topic from the time of its creation on application startup
    @KafkaListener(
            groupId = "group_id",
            topicPartitions = @TopicPartition(
                    topic = "Original XML",
                    partitionOffsets = { @PartitionOffset(
                            partition = "0",
                            initialOffset = "0") }))
    @SendTo("Ind Transactions")
    Map<String, String> listenToPartitionWithOffset(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset) throws ParserConfigurationException,
            IOException, SAXException, XPathExpressionException, TransformerException {
        logger.info("Received message [{}] from partition-{} with offset-{}",
                message,
                partition,
                offset);

        //TODO: split message to individual transactions and set to return
        Document doc = (Document) DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(message);
        XPath xPath = XPathFactory.newInstance().newXPath();
        XPathExpression exp = xPath.compile("//Engineer");
        NodeList nl = (NodeList)exp.evaluate(doc, XPathConstants.NODESET);
        Map<String, String> splitterMap = new HashMap<String, String>();
        for (int i = 0; i < nl.getLength(); i++) {
            Node node = nl.item(i);
            StringWriter buf = new StringWriter();
            Transformer xform = TransformerFactory.newInstance().newTransformer();
            xform.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            xform.transform(new DOMSource(node), new StreamResult(buf));
            System.out.println(buf.toString());
            splitterMap.put("Ind"+i+1, buf.toString());
        }
        return splitterMap;
    }

//    @KafkaListener(topics = "Original XML", groupId = "group_id")
//    public void consume(String message) throws IOException {
//        logger.info(String.format("#### -> Consumed message -> %s", message));
//    }
}
