package com.rarible.loader.cache.internal

import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

@Document
data class CacheEntry(
    @Id
    val key: String,
    val data: Any,
    val cachedAt: Instant,
    @Version
    val version: Long = 0
)
