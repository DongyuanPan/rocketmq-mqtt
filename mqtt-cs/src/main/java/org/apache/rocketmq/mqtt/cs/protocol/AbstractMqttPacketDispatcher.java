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

package org.apache.rocketmq.mqtt.cs.protocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.util.ReferenceCountUtil;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookManager;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.cs.channel.ChannelDecodeException;
import org.apache.rocketmq.mqtt.cs.channel.ChannelException;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

public abstract class AbstractMqttPacketDispatcher extends SimpleChannelInboundHandler<MqttMessage> {

    protected abstract Logger logger();

    protected abstract UpstreamHookManager upstreamHookManager();

    protected abstract boolean preHandler(ChannelHandlerContext ctx, MqttMessage msg);

    protected abstract void doChannelRead(ChannelHandlerContext ctx, MqttMessage msg, HookResult upstreamHookResult);

    protected abstract MqttMessageUpContext buildMqttMessageUpContext(ChannelHandlerContext ctx);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        if (!ctx.channel().isActive()) {
            return;
        }
        if (!msg.decoderResult().isSuccess()) {
            throw new ChannelDecodeException(ChannelInfo.getClientId(ctx.channel()) + "," + msg.decoderResult());
        }
        ChannelInfo.touch(ctx.channel());
        boolean preResult = preHandler(ctx, msg);
        if (!preResult) {
            return;
        }

        CompletableFuture<HookResult> upstreamHookResult;
        try {
            if (msg instanceof MqttPublishMessage) {
                ((MqttPublishMessage) msg).retain();
            }
            upstreamHookResult = upstreamHookManager().doUpstreamHook(buildMqttMessageUpContext(ctx), msg);
            if (upstreamHookResult == null) {
                doChannelRead(ctx, msg, null);
                return;
            }
        } catch (Throwable t) {
            logger().error("", t);
            if (msg instanceof MqttPublishMessage) {
                ReferenceCountUtil.release(msg);
            }
            throw new ChannelException(t.getMessage());
        }

        upstreamHookResult.whenComplete((hookResult, throwable) -> {
            if (msg instanceof MqttPublishMessage) {
                ReferenceCountUtil.release(msg);
            }
            if (throwable != null) {
                logger().error("", throwable);
                ctx.fireExceptionCaught(new ChannelException(throwable.getMessage()));
                return;
            }
            if (hookResult == null) {
                ctx.fireExceptionCaught(new ChannelException("UpstreamHook Result Unknown"));
                return;
            }
            try {
                doChannelRead(ctx, msg, hookResult);
            } catch (Throwable t) {
                logger().error("", t);
                ctx.fireExceptionCaught(new ChannelException(t.getMessage()));
            }
        });
    }
}
