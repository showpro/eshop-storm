package com.zhan.eshop.storm.spout;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.zhan.eshop.storm.constant.Constants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * spout从kafka获取数据
 */
public class AccessLogKafkaSpout extends BaseRichSpout {

    private static final long serialVersionUID = 8698470299234327074L;

    private static final Logger LOGGER = LoggerFactory.getLogger(AccessLogKafkaSpout.class);

    private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);

    private SpoutOutputCollector collector;

    private KafkaConsumer<String, String> consumer;

    private ConsumerRecords<String, String> msgList;

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        kafkaInitAndConsumer();
    }

    @Override
    public void nextTuple() {
        //4.循环消费
        while (true) {
            try {
                // 调用poll输出数据并提交offset
                msgList = consumer.poll(100);
                if (null != msgList && !msgList.isEmpty()) {
                    String message = "";
                    for (ConsumerRecord<String, String> record : msgList) {
                        // 原始数据
                        message = record.value();
                        if (null == message || "".equals(message.trim())) {
                            continue;
                        }
                        // 新启动一个线程去处理
                        new Thread(new KafkaMessageProcessor(record)).start();
                    }

                    LOGGER.info("【queue队列的数据】message=" + queue +"大小=" + queue.size());
                    if (queue.size() > 0) {
                        // 没有观察到Storm UI中的日志，不知道什么原因？
                        try {
                            // take(): 获取并移除此队列的头部，队列没有数据一直等待 。queue的长度 == 0 的时候，一直阻塞
                            String msg = queue.take();
                            collector.emit(new Values(msg));
                            LOGGER.info("【AccessLogKafkaSpout发射出去一条日志】message=" + msg);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        Utils.sleep(100);
                    }

                    // 异步提交
                    consumer.commitAsync();
                }
            } catch (Exception e) {
                LOGGER.error("消息队列处理异常!", e);
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e1) {
                    LOGGER.error("暂停失败!", e1);
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

    /**
     * 初始化kafka配置
     * 后期整合Spring Boot可以从配置文件中读取
     */
    private void kafkaInitAndConsumer() {
        //1.配置参数
        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.KAFKA_SERVERS);
        props.put("max.poll.records", 10);
        props.put("enable.auto.commit", false);
        props.put("group.id", "eshop-cache-group");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //2.创建1个消费者
        consumer = new KafkaConsumer<String, String>(props);
        String topic = Constants.TOPIC_NAME;
        //3.订阅topic
        this.consumer.subscribe(Arrays.asList(topic));
        LOGGER.info("消息队列[" + topic + "] 开始初始化...");
        // //4.循环消费, 放在这里消费，nextTuple()里从队列取数发射，不行，原因待分析
        // while (true) {
        //     try {
        //         // 调用poll输出数据并提交offset
        //         msgList = consumer.poll(100);
        //         if (null != msgList && !msgList.isEmpty()) {
        //             String message = "";
        //             for (ConsumerRecord<String, String> record : msgList) {
        //                 // 原始数据
        //                 message = record.value();
        //                 if (null == message || "".equals(message.trim())) {
        //                     continue;
        //                 }
        //                 // 新启动一个线程去处理
        //                 new Thread(new KafkaMessageProcessor(record)).start();
        //             }
        //             // 异步提交
        //             consumer.commitAsync();
        //         }
        //     } catch (Exception e) {
        //         LOGGER.error("消息队列处理异常!", e);
        //         try {
        //             TimeUnit.SECONDS.sleep(10);
        //         } catch (InterruptedException e1) {
        //             LOGGER.error("暂停失败!", e1);
        //         }
        //     }
        // }
    }

    /**
     * kafka消费者消息处理线程
     */
    private class KafkaMessageProcessor implements Runnable {

        private ConsumerRecord<String, String> record;

        public KafkaMessageProcessor(ConsumerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public void run() {
            String message = new String(record.value());
            try {
                queue.put(message);
                LOGGER.info("【AccessLogKafkaSpout中的Kafka消费者接收到一条日志】message=" + message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
