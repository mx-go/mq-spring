RocketMQ自动化配置。Spring与RocketMQ结合，简单封装，使用起来更加简单。

# 使用方法

引入Maven坐标

```xml
<dependency>
   <groupId>com.github.max</groupId>
   <artifactId>mq-spring</artifactId>
   <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## 消息订阅consumer

1. 使用构造方法(可选参数)

```xml
<!--消息消费者-->
<bean id="messageListener" class="com.open.rainbowhorse.mq.MessageListener"/>
<bean id="consumer" class="com.github.max.rocketmq.RocketMQProcessor" init-method="init" destroy-method="shutDown">
  <constructor-arg name="nameServer" value="127.0.0.1:9876"/>
  <constructor-arg name="groupName" value="groupName"/>
  <constructor-arg name="topics" value="topicKey:tag1||tag2,topicKey1"/>
  <constructor-arg name="listener" ref="messageListener"/>
  <constructor-arg name="consumeThreadMin" value="60"/>
  <constructor-arg name="consumeThreadMax" value="100"/>
</bean>
```

2. 复杂参数可使用`Configuration`配置对象。更多配置详见`Configuration`对象。

```xml
<!--消息消费者-->
<bean id="messageListener" class="com.open.rainbowhorse.mq.MessageListener"/>
<bean id="consumer" class="com.github.max.rocketmq.RocketMQProcessor" init-method="init" destroy-method="shutDown">
  <constructor-arg name="config" ref="config"/>
  <constructor-arg name="listener" ref="messageListener"/>
</bean>
<!-- 可配置更多参数，详见Configuration对象-->
<bean id="config" class="com.github.max.rocketmq.Configuration">
  <property name="nameServer" value="127.0.0.1:9876"/>
  <property name="groupName" value="groupName"/>
  <property name="topics" value="topicKey:tag1||tag2,topicKey1"/>
</bean>
```

## 消息生产producer

1. 使用构造方法

```xml
<bean id="sender" class="com.github.max.rocketmq.RocketMQSender" init-method="init" destroy-method="shutDown">
  <constructor-arg name="nameServer" value="127.0.0.1:9876"/>
  <constructor-arg name="groupName" value="groupName"/>
  <constructor-arg name="topics" value="topicKey"/>
</bean>
```

2. 复杂参数可使用`Configuration`配置对象。更多配置详见`Configuration`对象。

```xml
<bean id="sender" class="com.github.max.rocketmq.RocketMQSender" init-method="init" destroy-method="shutDown">
  <property name="config" ref="config"/>
</bean>
<!-- 可配置更多参数，详见Configuration对象-->
<bean id="config" class="com.github.max.rocketmq.Configuration">
  <property name="nameServer" value="127.0.0.1:9876"/>
  <property name="groupName" value="groupName"/>
  <property name="topics" value="topicKey"/>
</bean>
```

## 使用

消息生产注入`RocketMQSender`使用`sender`发送消息`Message`。

```java
@Autowired
private RocketMQSender sender;
```

