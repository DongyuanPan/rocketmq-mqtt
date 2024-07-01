/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.protocol.MqttPacketHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.facotry.MqttMessageFactory;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class Mqtt5PubRelHandler implements MqttPacketHandler<MqttMessage> {

    @Resource
    private InFlyCache inFlyCache;

    @Resource
    private SessionLoop sessionLoop;

    @Override
    public boolean preHandler(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        return true;
    }

    @Override
    public void doHandler(ChannelHandlerContext ctx, MqttMessage mqttMessage, HookResult upstreamHookResult) {
        final MqttPubReplyMessageVariableHeader variableHeader = (MqttPubReplyMessageVariableHeader) mqttMessage.variableHeader();
        String channelId = ChannelInfo.getId(ctx.channel());
        Session session = sessionLoop.getSession(channelId);

        if (!inFlyCache.contains(InFlyCache.CacheType.PUB, channelId, variableHeader.messageId())) {
            ctx.channel().writeAndFlush(MqttMessageFactory.buildMqtt5PubCompMessage(
                    variableHeader.messageId(),
                    MqttReasonCodes.PubComp.PACKET_IDENTIFIER_NOT_FOUND.byteValue(),
                    MqttProperties.NO_PROPERTIES));
            session.publishReceiveRefill();
            return;
        }

        inFlyCache.remove(InFlyCache.CacheType.PUB, channelId, variableHeader.messageId());
        ctx.channel().writeAndFlush(MqttMessageFactory.buildMqtt5PubCompMessage(
                variableHeader.messageId(),
                MqttReasonCodes.PubComp.SUCCESS.byteValue(),
                MqttProperties.NO_PROPERTIES));
        session.publishReceiveRefill();
    }
}