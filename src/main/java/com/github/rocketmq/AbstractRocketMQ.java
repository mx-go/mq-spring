package com.github.rocketmq;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

/**
 * @author: max
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

        log.info("====== start init rocketMQ config ======");
        config.setNameServer(nameServer);
        log.info("nameServer:" + nameServer);
        config.setGroupName(groupName);
        log.info("groupName:" + groupName);
        config.setTopics(topics);
        log.info("topics:" + topics);

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
     * 初始化方法
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
