package com.renren.kylin.component.rocketmq;

import com.renren.kylin.component.Result;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 消费消息handler
 * @author : peng.chen5@renren-inc.com
 * @Time : 2017/7/20 下午2:48
 */
public interface ConsumerMsgHandler {
    Result handleMsg(List<MessageExt> msgs);
}
