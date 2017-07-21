package com.renren.kylin.component.rocketmq;

import com.renren.kylin.component.Result;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Clustering Push 消费者
 * @author : peng.chen5@renren-inc.com
 * @Time : 2017/7/20 下午2:42
 */
public class PushConsumer {
    private String nameSrvAddrs;
    private String group;
    private String topic;
    private String tags;
    private ConsumerMsgHandler handler;

    public PushConsumer(List<String> nameSrvAddrs , String group, String topic, List<String> tags, ConsumerMsgHandler handler) {
        if(CollectionUtils.isNotEmpty(nameSrvAddrs)){
            this.nameSrvAddrs = "";
            for(String nameSrvAddr : nameSrvAddrs){
                if(StringUtils.isNotBlank(this.nameSrvAddrs)){
                    this.nameSrvAddrs += ";";
                }
                this.nameSrvAddrs += nameSrvAddr;
            }
        }
        this.group = group;
        this.topic = topic;
        if(CollectionUtils.isNotEmpty(tags)){
            this.tags = "";
            for(String tag : tags){
                if(StringUtils.isNotBlank(this.tags)){
                    this.tags += " || ";
                }
                this.tags += tag;
            }
        }
        this.handler = handler;
    }

    public PushConsumer(){}

    public void consume(){
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(nameSrvAddrs);
        try {
            consumer.subscribe(topic, tags);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    System.out.printf(Thread.currentThread().getName() + " 收到消息为: " + msgs + "%n");
                    Result result = handler.handleMsg(msgs);
                    if(result.isSuccess()){
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }else{
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
            });
            consumer.start();
            System.out.println("Consumer Started.");
        } catch (MQClientException e) {
            e.printStackTrace();
        }

    }
}
