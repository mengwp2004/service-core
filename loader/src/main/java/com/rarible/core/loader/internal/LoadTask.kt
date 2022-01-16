package com.rarible.core.loader.internal

import com.rarible.core.loader.LoadType
import java.time.Instant

data class LoadTask(
    val type: LoadType,
    val key: String,
    val attempts: Int,
    val scheduledAt: Instant
)
