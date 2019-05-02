package com.github.max.rocketmq;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * @description: RocketMQ发消息
 * @author: max
 * @date: 2019-05-02 16:57
 **/
@Slf4j
public class RocketMQSender extends AbstractRocketMQ {

    private volatile DefaultMQProducer sender;

    public RocketMQSender() {
    }

    public RocketMQSender(String nameServer, String groupName, String topics) {
        super(nameServer, groupName, topics, 0, 0);
    }

    /**
     * 配置发生变化时加载
     * sender volatile 并不保证线程安全，有丢消息风险。
     * 尽量不要依赖动态变更
     */
    @Override
    public void load() {
        if (sender == null) {
            createProducer();
        } else {
            shutDownProducer();
            createProducer();
        }
    }

    /**
     * 同步消息发送
     */
    public SendResult send(Message message) {
        if (message.getTopic() == null && getTopic() != null) {
            message.setTopic(getTopic());
            return doSend(message, null, null);
        } else if (message.getTopic() != null) {
            return doSend(message, null, null);
        } else {
            log.warn("DiscardNoTopicMessage");
            return null;
        }
    }

    private SendResult doSend(Message msg, MessageQueueSelector selector, Object arg) {
        SendResult result;
        try {
            result = (selector == null ? sender.send(msg) : sender.send(msg, selector, arg));
            SendStatus status = result.getSendStatus();
            if (status.equals(SendStatus.SEND_OK)) {
                log.debug("msgId={}, status={}", result.getMsgId(), status);
            } else {
                log.error("msgId={}, status={}", result.getMsgId(), status);
            }
            return result;
        } catch (Exception e) {
            log.error("SendError, message={}", msg, e);
            return null;
        }
    }

    /**
     * 支持筛选队列
     */
    public SendResult send(Message message, MessageQueueSelector selector, Object arg) {
        if (message.getTopic() == null && getTopic() != null) {
            message.setTopic(getTopic());
            return doSend(message, selector, arg);
        } else if (message.getTopic() != null) {
            return doSend(message, selector, arg);
        } else {
            log.warn("DiscardNoTopicMessage");
            return null;
        }
    }

    private void createProducer() {
        try {
            sender = new DefaultMQProducer(config.getGroupName());
            sender.setNamesrvAddr(config.getNameServer());
            sender.setMaxMessageSize(config.getMaxMessageSize());
            sender.setInstanceName(UUID.randomUUID().toString());
            sender.start();
        } catch (MQClientException e) {
            log.error("CanNotCreateProducer nameServer={} group={} ", config.getNameServer(), config.getGroupName(), e);
        }
    }

    private void shutDownProducer() {
        if (this.sender != null) {
            try {
                this.sender.shutdown();
                this.sender = null;
            } catch (Exception e) {
                log.error("ShutRocketMQDownProducer Error, nameServer={} group={}", sender.getNamesrvAddr(),
                        sender.getProducerGroup(), e);
            }
        }
    }

    @Override
    public void shutDown() {
        shutDownProducer();
    }

    public Map<String, String> getTopics() {
        return Collections.unmodifiableMap(config.getTopics());
    }

    private String getTopic() {
        if (config.getTopics() != null) {
            return config.getTopics().keySet().iterator().next();
        }
        return null;
    }
}

