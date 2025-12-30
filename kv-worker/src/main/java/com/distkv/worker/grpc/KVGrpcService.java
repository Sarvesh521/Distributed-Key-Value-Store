package com.distkv.worker.grpc;

import com.distkv.grpc.*;
import com.distkv.worker.model.KVEntry;
import com.distkv.worker.repository.KVRepository;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@GrpcService
@RequiredArgsConstructor
public class KVGrpcService extends KVServiceGrpc.KVServiceImplBase {

    private final KVRepository repository;

    @Override
    @Transactional
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        KVEntry entry = KVEntry.builder()
                .key(request.getKey())
                .value(request.getValue())
                .vectorClock(request.getVectorClockMap())
                .build();
        repository.save(entry);
        
        responseObserver.onNext(PutResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        Optional<KVEntry> entryOpt = repository.findById(request.getKey());
        GetResponse.Builder responseBuilder = GetResponse.newBuilder();
        
        if (entryOpt.isPresent()) {
            KVEntry entry = entryOpt.get();
            responseBuilder.setValue(entry.getValue())
                    .putAllVectorClock(entry.getVectorClock())
                    .setFound(true);
        } else {
            responseBuilder.setFound(false);
        }
        
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    @Transactional
    public void replicate(ReplicateRequest request, StreamObserver<ReplicateResponse> responseObserver) {
        KVEntry entry = KVEntry.builder()
                .key(request.getKey())
                .value(request.getValue())
                .vectorClock(request.getVectorClockMap())
                .build();
        repository.save(entry);
        
        responseObserver.onNext(ReplicateResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void sync(SyncRequest request, StreamObserver<SyncEntry> responseObserver) {
        // Simple full sync for now, can be optimized with vector clock comparison
        repository.findAll().forEach(entry -> {
            responseObserver.onNext(SyncEntry.newBuilder()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue())
                    .putAllVectorClock(entry.getVectorClock())
                    .build());
        });
        responseObserver.onCompleted();
    }
}
