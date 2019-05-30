package app.itw.rocketmq.demo1.consume;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * 消费者
 *
 * @Author: Tison
 * @create: 2019-05-28 15:22
 */

@Controller
public class ConsumeService {

  //mq服务器集群
  public static final String namesrvAddr = "192.168.186.132:9876;192.168.132.141:9876";

  @Value("${server.port}")
  private  static String hostName;

  DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TestMessageGroup");

  ConsumeService() throws MQClientException {
    // Specify name server addresses.
    consumer.setNamesrvAddr(namesrvAddr);
    //Launch the instance.

    //-----------消息过滤

    //这是接收所有的tage标签的消息
    consumer.subscribe("TopicTest", "*");


    //通过tage标签去消费指定的消息  || 或运算符
    //consumer.subscribe("TopicTest", "orderDeal || orderLog");

    //push支持SQL限制tag，pull不能使用SQL92
    //通过SQL92的方式消费指定的消息 sql where 的条件
    //consumer.subscribe("TopicTest", MessageSelector.bySql("qty>=1 && qty < 3") );

    //-----------消息过滤

    //-----------集群消费模式和广播消费模式
    consumer.setMessageModel(MessageModel.CLUSTERING);//只能有一个消费者消费（默认）
    //consumer.setMessageModel(MessageModel.BROADCASTING);//广播模式所有的消费者都会消费一次

    //-----------集群消费模式和广播消费模式


    //监听消息
    consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
      msgs.forEach(msg->{
        System.out.println(Thread.currentThread().getName()+"-消费消息：id="+ msg.getMsgId()+",tage="+msg.getTags()+",key="+msg.getKeys()+"，body="+new String(msg.getBody()));
      });
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });
    consumer.start();
    System.out.printf("消费端初始化完成");
  }


  @GetMapping("close")
  public void shutdown() {
    consumer.shutdown();
  }

}
