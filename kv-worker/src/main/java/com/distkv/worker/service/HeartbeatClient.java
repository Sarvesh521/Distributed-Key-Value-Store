package com.distkv.worker.service;

import com.distkv.grpc.HealthServiceGrpc;
import com.distkv.grpc.HeartbeatRequest;
import com.distkv.grpc.HeartbeatResponse;
import com.distkv.grpc.KVServiceGrpc;
import com.distkv.worker.repository.KVRepository;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class HeartbeatClient {

    @Autowired
    private KVRepository repository;

    @Value("${controller.host:kv-controller}")
    private String controllerHost;

    @Value("${controller.port:9090}")
    private int controllerPort;

    @Value("${worker.id}")
    private String workerId;

    @Value("${worker.address}")
    private String workerAddress;

    @Value("${grpc.server.port}")
    private int workerPort;

    private HealthServiceGrpc.HealthServiceStub asyncStub;

    @PostConstruct
    public void init() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(controllerHost, controllerPort)
                .usePlaintext()
                .build();
        asyncStub = HealthServiceGrpc.newStub(channel);
        startHeartbeat();
    }

    private void startHeartbeat() {
        StreamObserver<HeartbeatResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(HeartbeatResponse value) {
                if (value.getSyncFromAddress() != null && !value.getSyncFromAddress().isEmpty()) {
                    triggerSync(value.getSyncFromAddress(), value.getSyncFromPort());
                }
            }

            private void triggerSync(String host, int port) {
                log.info("Triggering sync from {}:{}", host, port);
                ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext()
                        .build();
                KVServiceGrpc.KVServiceStub stub = KVServiceGrpc.newStub(channel);
                stub.sync(com.distkv.grpc.SyncRequest.newBuilder().setWorkerId(workerId).build(), new StreamObserver<>() {
                    @Override
                    public void onNext(com.distkv.grpc.SyncEntry value) {
                        repository.save(com.distkv.worker.model.KVEntry.builder()
                                .key(value.getKey())
                                .value(value.getValue())
                                .vectorClock(value.getVectorClockMap())
                                .build());
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Sync failed: {}", t.getMessage());
                        channel.shutdown();
                    }

                    @Override
                    public void onCompleted() {
                        log.info("Sync completed");
                        channel.shutdown();
                    }
                });
            }

            @Override
            public void onError(Throwable t) {
                // Restart heartbeat on error
                try {
                    TimeUnit.SECONDS.sleep(5);
                    startHeartbeat();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onCompleted() {
                // Should not happen
            }
        };

        StreamObserver<HeartbeatRequest> requestObserver = asyncStub.heartbeat(responseObserver);
        
        new Thread(() -> {
            while (true) {
                try {
                    requestObserver.onNext(HeartbeatRequest.newBuilder()
                            .setWorkerId(workerId)
                            .setAddress(workerAddress)
                            .setPort(workerPort)
                            .build());
                    TimeUnit.SECONDS.sleep(2);
                } catch (Exception e) {
                    break;
                }
            }
        }).start();
    }
}
