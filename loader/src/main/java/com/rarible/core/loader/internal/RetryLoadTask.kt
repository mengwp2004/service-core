package com.rarible.core.loader.internal

import com.rarible.core.loader.LoadType
import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

@Document
data class RetryLoadTask(
    val type: LoadType,
    val key: String,
    val attempts: Int,
    val scheduledAt: Instant,
    val retryAt: Instant,
    @Version
    val version: Long = 0,
    @Id
    val id: String = ObjectId().toHexString()
)
