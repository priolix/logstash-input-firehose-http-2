package org.logstash.plugins.inputs.http;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.netty.util.CharsetUtil;


import static io.netty.buffer.Unpooled.copiedBuffer;

/**
 * Created by joaoduarte on 11/10/2017.
 */
public class HttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private final static Logger logger = LogManager.getLogger(HttpServerHandler.class);

    private final IMessageHandler messageHandler;
    private final ThreadPoolExecutor executorGroup;
    private final HttpResponseStatus responseStatus;
    private String responseBody;

    public HttpServerHandler(IMessageHandler messageHandler, ThreadPoolExecutor executorGroup,
                             HttpResponseStatus responseStatus, String responseBody) {
        this.messageHandler = messageHandler;
        this.executorGroup = executorGroup;
        this.responseStatus = responseStatus;
        this.responseBody = responseBody;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
        final String remoteAddress = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
        msg.retain();
        String content = msg.content().toString(CharsetUtil.UTF_8);
        // If the user agent in the content is User-Agent: Amazon Kinesis Data Firehose Agent/1.0 and Content-Type: application/json and a X-Amz-Firehose-Request-Id header is present,
        // then we need to set the responseBody with the requestId and timestamp that are in the content
        if (msg.headers().get(HttpHeaderNames.USER_AGENT).equals("Amazon Kinesis Data Firehose Agent/1.0") &&
            msg.headers().get(HttpHeaderNames.CONTENT_TYPE).equals("application/json") &&
            msg.headers().contains("X-Amz-Firehose-Request-Id")) {
            // Extract requestId from the headers
            String requestId = msg.headers().get("X-Amz-Firehose-Request-Id");
            // Extract timestamp from the content
            String timestamp = "1578090903599";
            responseBody = String.format("{\"requestId\":\"%s\", \"timestamp\":\"%s\"}", requestId, timestamp);
            logger.debug("We entered the special case for Amazon Kinesis Data Firehose Agent with requestId: {} and timestamp: {}", requestId, timestamp);
        }
        else {
            // If the user agent is not the one we expect, we just use the responseBody as it is
            logger.debug("User-Agent is not Amazon Kinesis Data Firehose Agent/1.0, using default responseBody: {}", responseBody);
        }
        final MessageProcessor messageProcessor = new MessageProcessor(ctx, msg, remoteAddress, messageHandler, responseStatus, responseBody);
        // Print all ctx, msg, remoteAddress, messageHandler, responseStatus, responseBody) 
        logger.debug("Processing message from {} with message {}", remoteAddress, content);
        logger.debug("FullHttpRequest: {}", msg);
        logger.debug("ctx: {}", ctx);
        logger.debug("MessageHandler: {}", messageHandler);
        logger.debug("Response status: {}", responseStatus);
        logger.debug("Response body: {}", responseBody);
        executorGroup.execute(messageProcessor);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        final ByteBuf content = copiedBuffer(cause.getMessage().getBytes());
        final HttpResponseStatus responseStatus;

        if (cause instanceof DecompressionException) {
            responseStatus = HttpResponseStatus.BAD_REQUEST;
        } else {
            responseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }
        final DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                responseStatus,
                content
        );
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
        ctx.writeAndFlush(response);
    }
}