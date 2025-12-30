package com.distkv.controller.grpc;

import com.distkv.controller.registry.WorkerRegistry;
import com.distkv.grpc.HealthServiceGrpc;
import com.distkv.grpc.HeartbeatRequest;
import com.distkv.grpc.HeartbeatResponse;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
@RequiredArgsConstructor
public class HealthGrpcService extends HealthServiceGrpc.HealthServiceImplBase {

    private final WorkerRegistry registry;

    @Override
    public StreamObserver<HeartbeatRequest> heartbeat(StreamObserver<HeartbeatResponse> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(HeartbeatRequest request) {
                boolean isNew = !registry.getActiveWorkers().containsKey(request.getWorkerId());
                registry.registerHeartbeat(request.getWorkerId(), request.getAddress(), request.getPort());
                
                HeartbeatResponse.Builder response = HeartbeatResponse.newBuilder().setStatus("OK");
                
                if (isNew && !registry.getActiveWorkers().isEmpty()) {
                    // Tell worker to sync from someone else
                    registry.getActiveWorkers().entrySet().stream()
                            .filter(e -> !e.getKey().equals(request.getWorkerId()))
                            .findFirst()
                            .ifPresent(e -> {
                                response.setSyncFromAddress(e.getValue().getAddress());
                                response.setSyncFromPort(e.getValue().getPort());
                            });
                }
                
                responseObserver.onNext(response.build());
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }
}
