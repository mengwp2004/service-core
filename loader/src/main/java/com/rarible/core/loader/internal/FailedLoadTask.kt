package com.rarible.core.loader.internal

import com.rarible.core.loader.LoadType
import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import java.time.Instant

data class FailedLoadTask(
    val type: LoadType,
    val key: String,
    val attempts: Int,
    val scheduledAt: Instant,
    val lastFailedAt: Instant,
    @Id
    val id: String = ObjectId().toHexString()
)
