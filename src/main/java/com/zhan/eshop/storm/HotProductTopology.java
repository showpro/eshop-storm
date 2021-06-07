package com.zhan.eshop.storm;

import com.alibaba.fastjson.JSONObject;
import com.zhan.eshop.storm.bolt.LogParseBolt;
import com.zhan.eshop.storm.bolt.ProductCountBolt;
import com.zhan.eshop.storm.constant.Constants;
import com.zhan.eshop.storm.kafka.KafkaProducerUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import com.zhan.eshop.storm.spout.AccessLogKafkaSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 热数据统计拓扑
 *
 * @author zhanzhan
 * @date 2021/6/6 21:18
 */
public class HotProductTopology {

    private final static Logger logger = LoggerFactory.getLogger(HotProductTopology.class);

    public static void main(String[] args) {
        // 定义一个拓扑
        TopologyBuilder builder = new TopologyBuilder();
        // 设置1个Executeor(线程)，默认一个
        builder.setSpout(Constants.KAFKA_SPOUT, new AccessLogKafkaSpout(), 1);
        builder.setBolt("LogParseBolt", new LogParseBolt(), 2)
                .setNumTasks(2)
                .shuffleGrouping(Constants.KAFKA_SPOUT);
        builder.setBolt("ProductCountBolt", new ProductCountBolt(), 2)
                .setNumTasks(2)
                .fieldsGrouping("LogParseBolt", new Fields("productId"));

        Config config = new Config();

        try {
            // 运行拓扑
            // 有参数时，表示向集群提交作业，并把第一个参数当做topology名称
            // 没有参数时，本地提交
            if (args != null && args.length > 0) {
                //设置3个work
                config.setNumWorkers(3);
                try {
                    logger.info("运行远程模式");
                    StormSubmitter.submitTopology(args[0], config, builder.createTopology());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                // 启动本地模式
                logger.info("运行本地模式");
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("HotProductTopology", config, builder.createTopology());
                Utils.sleep(30000);
                cluster.shutdown();
            }
        } catch (Exception e) {
            logger.error("storm启动失败!程序退出!", e);
            System.exit(1);
        }

        //启动往kafka发送数据
        sendMsg();
        logger.info("storm启动成功...");
    }

    /**
     * 启动往kafka发送数据
     */
    private static void sendMsg() {
        List<String> list = new ArrayList<String>();
        for (int i = 1; i <= 10; i++) {
            JSONObject json = new JSONObject();
            json.put("id", i);
            json.put("name", "张三_" + i);
            json.put("age", i);
            list.add(json.toJSONString());
        }
        KafkaProducerUtil.sendMessage(list, Constants.KAFKA_SERVERS, Constants.TOPIC_NAME);
    }
}
