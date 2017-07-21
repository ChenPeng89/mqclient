package com.renren.kylin.component.rocketmq;

import com.renren.kylin.component.Result;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import java.util.List;

/**
 * 异步producer
 * @author : peng.chen5@renren-inc.com
 * @Time : 2017/7/20 下午12:03
 */
public class AsyncProducer {

    private String nameSrvAddrs;
    private String group;
    private String topic;
    private String tag;

    public AsyncProducer(){}

    public AsyncProducer(List<String> nameSrvAddrs ,String group , String topic , String tag){
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
        this.tag = tag;

    }

    /**
     * 异步发送消息
     * @param key           消息key
     * @param message       消息体
     * @return
     */
    public Result send(String key , String message){
        DefaultMQProducer producer = new DefaultMQProducer(group);
        producer.setNamesrvAddr(this.nameSrvAddrs);
        producer.setRetryTimesWhenSendAsyncFailed(5);
        try {
            producer.start();
            Message msg = new Message(topic,
                    tag,
                    key,
                    message.getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            return Result.fail(e.getMessage());
        }finally {
            producer.shutdown();
        }
        return Result.success();
    }
}
