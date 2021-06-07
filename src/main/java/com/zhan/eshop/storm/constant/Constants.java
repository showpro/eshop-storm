package com.zhan.eshop.storm.constant;

/**
 * @Description 常量类
 * @Author zhanzhan
 * @Date 2021/6/7 9:34
 */
public class Constants {
    public  final static String KAFKA_SPOUT="AccessLogKafkaSpout";

    public  final static String BOLT="INSERT_BOLT";

    public  final static String FIELD="insert";

    public  final static String KAFKA_SERVERS="192.168.133.133:9092,192.168.133.128:9092,192.168.133.129:9092";

    public  final static String TOPIC_NAME="access-log";
}
