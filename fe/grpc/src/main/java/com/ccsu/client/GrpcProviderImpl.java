/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ccsu.client;

import com.ccsu.system.EndPoint;
import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

public class GrpcProviderImpl
        implements GrpcProvider {

    private static final int MAX_RETRY = 10;

    private final LoadingCache<EndPoint, ManagedChannel> managedChannels;

    public GrpcProviderImpl() {
        this.managedChannels =
                CacheBuilder.newBuilder()
                        .removalListener(
                                (RemovalListener<EndPoint, ManagedChannel>) notification -> {
                                    if (!notification.wasEvicted()) {
//                                        BackEnd.close(notification.getValue());
                                        System.out.println("s");
                                    }
                                })
                        .build(
                                new CacheLoader<EndPoint, ManagedChannel>() {
                                    @Override
                                    public ManagedChannel load(
                                            EndPoint endPoint) {
                                        final NettyChannelBuilder builder =
                                                NettyChannelBuilder.forAddress(
                                                                endPoint.getHost(),
                                                                endPoint.getPort())
                                                        .maxInboundMessageSize(Integer.MAX_VALUE)
                                                        .maxInboundMetadataSize(Integer.MAX_VALUE)
                                                        .enableRetry()
//                                                        .intercept(
//                                                                TracingClientInterceptor
//                                                                        .newBuilder()
//                                                                        .withTracer(
//                                                                                TracerFacade
//                                                                                        .INSTANCE
//                                                                                        .getTracer())
//                                                                        .build())
//                                                        .keepAliveTimeout(backEnd.getGrpcKe
//                                                        epAliveTimeout(), TimeUnit.SECONDS)
//                                                        .keepAliveTime(backEnd.getGrpcKeepAliveTime(),
//                                                        TimeUnit.SECONDS)
                                                        .keepAliveWithoutCalls(true)
                                                        .maxRetryAttempts(
                                                                MAX_RETRY);

                                        builder.usePlaintext();
                                        return builder.build();
                                    }
                                });
    }

    @Override
    public ManagedChannel getOrCreateChannel(EndPoint endPoint) {
        requireNonNull(endPoint, "peerEndpoint is required");
        try {
            return managedChannels.get(endPoint);
        } catch (ExecutionException e) {
            throw new CommonException(CommonErrorCode.FILE_ERROR, e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        managedChannels.invalidateAll(); // shutdowns all connections
    }
}
