package com.zhan.eshop.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import com.zhan.eshop.storm.spout.AccessLogKafkaSpout;

/**
 * 热数据统计拓扑
 *
 * @author zhanzhan
 * @date 2021/6/6 21:18
 */
public class HotProductTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("AccessLogKafkaSpout", new AccessLogKafkaSpout(), 1);
        builder.setBolt("LogParseBolt", new LogParseBolt(), 2)
            .setNumTasks(2)
            .shuffleGrouping("AccessLogKafkaSpout");
        builder.setBolt("ProductCountBolt", new ProductCountBolt(), 2)
            .setNumTasks(2)
            .fieldsGrouping("LogParseBolt", new Fields("productId"));

        Config config = new Config();

        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HotProductTopology", config, builder.createTopology());
            Utils.sleep(30000);
            cluster.shutdown();
        }
    }
}
