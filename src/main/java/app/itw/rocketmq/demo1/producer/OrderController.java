package app.itw.rocketmq.demo1.producer;

import app.itw.rocketmq.demo1.consume.ConsumeService;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 测试发送消息
 *
 * @Author: Tison
 * @create: 2019-05-28 15:24
 */
@RestController
@RequestMapping("test")
public class OrderController {

  static DefaultMQProducer producer = new DefaultMQProducer("TestMessageGroup");

  OrderController() throws MQClientException {
    // Specify name server addresses.
    producer.setNamesrvAddr(ConsumeService.namesrvAddr);
    producer.start();
    //Launch the instance.
  }

  @GetMapping("close")
  public Object shutdown() {
    producer.shutdown();
    return "ok";
  }


  /**
   * 同步发送消息。
   */

  @GetMapping("send1")
  public Object test1(String text) throws Exception {
    String oid = "oid" + System.currentTimeMillis();
    System.out.println(oid);
    Message msg = new Message("TopicTest", "orderDeal", oid,
        (text).getBytes(RemotingHelper.DEFAULT_CHARSET));

    //Call send message to deliver message to one of brokers.
    SendResult sendResult = producer.send(msg);
    System.out.println(msg);
    System.out.println(sendResult);
    return sendResult;
  }

  /**
   * 异步发送消息。
   */

  @GetMapping("send2")
  public Object test2(String text) throws Exception {
    //订单id
    String oid = "oid" + System.currentTimeMillis();
    System.out.println(oid);
    Message msg = new Message("TopicTest", "orderDeal", oid,
        (text).getBytes(RemotingHelper.DEFAULT_CHARSET));
    //异步发送失败的话，rocketmq内部重试多少次 ,源码默认2次
    producer.setRetryTimesWhenSendAsyncFailed(2);
    producer.send(msg, new SendCallback() {
      @Override
      public void onSuccess(SendResult sendResult) {
        System.out.printf("异步发送成功：" + sendResult);
      }

      @Override
      public void onException(Throwable e) {
        System.out.printf("异步发送失败：");
        e.printStackTrace();
      }
    });
    System.out.println(msg);
    return msg;
  }


  /**
   * 这种方式通常用于日志收集， 只管发出去，不考虑丢失和失败。
   */
  @GetMapping("send3")
  public Object OnewayProducer(String text) throws Exception {
    Message msg = new Message("TopicTest", "orderLog",
        (text).getBytes(RemotingHelper.DEFAULT_CHARSET)
    );
    producer.sendOneway(msg);
    System.out.println(msg);
    return "ok";
  }

  /**
   * 同步发送消息。
   */

  @GetMapping("send4")
  public Object MessageQueueSelector(String oid) throws Exception {
    //String oid = "oid213" ;//+ System.currentTimeMillis();
    System.out.println(oid);
    Message msg = new Message("TopicTest", "orderDeal", oid,
        (oid).getBytes(RemotingHelper.DEFAULT_CHARSET));
    //发送消息到指定的Queue队列（默认一个Topic有4个Queue，我们指定发送给某一个）
    SendResult sendResult = producer.send(msg, (mqs, msg1, arg) -> {
      //从外面传递订单id进来，进行取模运算，投递到指定的队列。
      int index = arg.hashCode() % mqs.size();
      //字符串hashCode有可能是负数
      return mqs.get(Math.abs(index));
    }, oid);
    System.out.println(msg);
    System.out.println(sendResult);
    return sendResult;
  }


}
