package com.renren.kylin.api;

import com.renren.kylin.component.Result;
import com.renren.kylin.component.rocketmq.AsyncProducer;
import com.renren.kylin.component.rocketmq.BroadcastPushConsumer;
import com.renren.kylin.component.rocketmq.ConsumerMsgHandler;
import com.renren.kylin.component.rocketmq.PushConsumer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.ArrayList;
import java.util.List;

/**
 * @author : peng.chen5@renren-inc.com
 * @Time : 2017/7/20 下午3:22
 */
public class RocketMqClient {
    public AsyncProducer getAsyncProducer(List<String> nameSrvAddrs , String group , String topic , String tag){
        return new AsyncProducer(nameSrvAddrs , group , topic , tag);
    }

    public PushConsumer getPushConsumer(List<String> nameSrvAddrs , String group, String topic, List<String> tags, ConsumerMsgHandler handler){
        return new PushConsumer(nameSrvAddrs , group , topic , tags , handler);
    }

    public BroadcastPushConsumer getBroadcastPushConsumer(List<String> nameSrvAddrs , String group, String topic, List<String> tags, ConsumerMsgHandler handler){
        return new BroadcastPushConsumer(nameSrvAddrs , group , topic , tags , handler);
    }


    public static void main(String[] args) {
        RocketMqClient client = new RocketMqClient();
        List<String> nameSrvAddr = new ArrayList<>(2);
        nameSrvAddr.add("localhost:9879");
        nameSrvAddr.add("localhost:9876");
        String group = "group1";
        String topic = "TopicTest";
        List<String> tags = new ArrayList<>(2);
        tags.add("TagA");
        AsyncProducer producer = client.getAsyncProducer(nameSrvAddr , group , topic , "TagA");
        for(int i = 0 ; i < 10 ; i ++){
            Result result = producer.send("key:" + i , "哇哇哇哇哇哇哇哇哇哇" + i);
            System.out.println(result);
        }
        tags.add("TagC");
        tags.add("TagB");

        PushConsumer consumer = client.getPushConsumer(nameSrvAddr, group, topic, tags, new ConsumerMsgHandler() {
            @Override
            public Result handleMsg(List<MessageExt> msgs) {
                if(CollectionUtils.isNotEmpty(msgs)){
                    for(MessageExt msg : msgs){
                        System.out.println(new String(msg.getBody()));
                    }
                }
                return Result.success();
            }
        });
        consumer.consume();
    }
}
