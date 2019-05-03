package com.github.max.rocketmq;

import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @description: RocketMQ配置信息
 * @author: max
 * @date: 2019-05-02 16:57
 **/
@Data
public class Configuration {

    /**
     * NameServer地址列表，多个nameServer地址用分号隔开。
     * 例：172.0.0.1:9876;172.0.0.2:9876;172.0.0.3:9876
     */
    private String nameServer;
    /**
     * 组名
     */
    private String groupName;
    /**
     * 主题。
     * 支持订阅多个主题和tag。例：topic1:tag1||tag2,topic2
     */
    private String topics;

    /**
     * 消息模型，支持以下两种1.集群消费2.广播消费
     * 缺省：MessageModel.CLUSTERING
     *
     * @see org.apache.rocketmq.common.protocol.heartbeat.MessageModel
     */
    private String messageModel;
    /**
     * 客户端限制的消息大小，超过报错，同时服务端也会限制（默认128K）
     */
    private int maxMessageSize = 1024 * 1024;
    /**
     * 消费线程池最小数量
     */
    private int consumeThreadMin = 20;
    /**
     * 消费线程池最大数量
     */
    private int consumeThreadMax = 64;
    /**
     * 批量拉消息，一次最多拉多少条。缺省：32
     */
    private int fetchSize = 10;
    /**
     * 批量消费，一次消费多少条消息
     */
    private int consumeMessageBatchMaxSize = 1;
    /**
     * 拉消息间隔，由于是长轮询，所以为0，但是如果应用了流控，也可以设置大于0的值，单位毫秒.
     */
    private long pullInterval = 0;

    Map<String, String> getTopics() {
        if (StringUtils.isBlank(this.topics)) {
            return Collections.emptyMap();
        }
        Map<String, String> result = Maps.newHashMap();
        Stream.of(StringUtils.split(this.topics, ",")).forEach(topic -> {
            String[] entry = StringUtils.split(topic, ":");
            if (entry.length < 1) {
                return;
            }
            result.put(entry[0], entry.length < 2 ? "*" : entry[1]);
        });
        return result;
    }
}
