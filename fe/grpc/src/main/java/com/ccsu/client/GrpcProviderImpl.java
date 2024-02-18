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

import com.ccsu.system.BackEndConfig;
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

    private final LoadingCache<BackEndConfig, ManagedChannel> managedChannels;

    public GrpcProviderImpl() {
        this.managedChannels =
                CacheBuilder.newBuilder()
                        .removalListener(
                                (RemovalListener<BackEndConfig, ManagedChannel>) notification -> {
                                })
                        .build(
                                new CacheLoader<BackEndConfig, ManagedChannel>() {
                                    @Override
                                    public ManagedChannel load(
                                            BackEndConfig backEndConfig) {
                                        final NettyChannelBuilder builder =
                                                NettyChannelBuilder.forAddress(
                                                                backEndConfig.getHost(),
                                                                backEndConfig.getPort())
                                                        .maxInboundMessageSize(Integer.MAX_VALUE)
                                                        .maxInboundMetadataSize(Integer.MAX_VALUE)
                                                        .enableRetry()
                                                        .keepAliveWithoutCalls(true)
                                                        .maxRetryAttempts(
                                                                MAX_RETRY);

                                        builder.usePlaintext();
                                        return builder.build();
                                    }
                                });
    }

    @Override
    public ManagedChannel getOrCreateChannel(BackEndConfig backEndConfig) {
        requireNonNull(backEndConfig, "peerEndpoint is required");
        try {
            return managedChannels.get(backEndConfig);
        } catch (ExecutionException e) {
            throw new CommonException(CommonErrorCode.FILE_ERROR, e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        // shutdowns all connections
        managedChannels.invalidateAll();
    }
}
