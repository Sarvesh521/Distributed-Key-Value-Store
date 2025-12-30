package com.distkv.worker.model;

import io.hypersistence.utils.hibernate.type.json.JsonBinaryType;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Type;

import java.util.Map;

@Entity
@Table(name = "kv_store")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KVEntry {
    @Id
    private String key;

    @Column(columnDefinition = "TEXT")
    private String value;

    @Type(JsonBinaryType.class)
    @Column(columnDefinition = "jsonb")
    private Map<String, Long> vectorClock;
}
