package com.github.max.rocketmq;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

/**
 * @description: RocketMQ
 * @author: max
 * @date: 2019-05-02 16:57
 **/
@Slf4j
public abstract class AbstractRocketMQ {

    /**
     * 存储配置信息的对象
     */
    @Getter
    @Setter
    protected volatile Configuration config = new Configuration();

    public AbstractRocketMQ() {

    }

    public AbstractRocketMQ(String nameServer, String groupName, String topics, int consumeThreadMin,
                            int consumeThreadMax) {
        if (StringUtils.isBlank(nameServer) || StringUtils.isBlank(groupName)) {
            throw new IllegalArgumentException(String.format("nameServer=%s,groupName=%s", nameServer, groupName));
        }
        config.setNameServer(nameServer);
        config.setGroupName(groupName);
        config.setTopics(topics);

        config.setMaxMessageSize(1024 * 1024);
        if (consumeThreadMin > 0) {
            config.setConsumeThreadMin(consumeThreadMin);
            log.info("consumeThreadMin:" + config.getConsumeThreadMin());
        }
        if (consumeThreadMax > 0) {
            config.setConsumeThreadMax(consumeThreadMax);
            log.info("consumeThreadMax:" + config.getConsumeThreadMax());
        }
    }

    /**
     * 初始化
     */
    public void init() {
        load();
    }

    /**
     * 提供给子类初始化
     */
    protected abstract void load();

    protected abstract void shutDown();
}
