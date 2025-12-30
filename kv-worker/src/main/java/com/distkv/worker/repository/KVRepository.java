package com.distkv.worker.repository;

import com.distkv.worker.model.KVEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface KVRepository extends JpaRepository<KVEntry, String> {
}
