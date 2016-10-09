# JMS-Redis
基于 Redis 实现的羽量级 Java 消息服务，仅支持 JMS 2.0 的部分特性

## 支持特性

* 支持 JMS 的基本消息类型

## 不支持特性

* 不支持 JMS 1.x 消息发送/接收相关的 API
* 不支持事务
* 不支持设置 DisableMessageID，消息默认会生成一个消息ID，格式为`ID:Base64压缩后的UUID:流水号`
* 不支持 MessageSelector
* 暂不支持优先级队列
* 暂不支持延迟发送 (DeliveryDelay)
