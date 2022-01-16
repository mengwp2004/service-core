package com.rarible.loader.cache.internal

import com.rarible.core.loader.LoadNotification
import com.rarible.core.loader.LoadNotificationListener
import com.rarible.core.loader.LoadType
import com.rarible.loader.cache.CacheType
import org.slf4j.LoggerFactory

/**
 * No-op listener of loading cache entries that simply logs outcome of loading tasks as additional diagnostics information.
 */
class LoggingLoaderNotificationListener(
    cacheType: CacheType
) : LoadNotificationListener {

    private val logger = LoggerFactory.getLogger(LoggingLoaderNotificationListener::class.java)

    override val type: LoadType = encodeLoadType(cacheType)

    override suspend fun onLoadNotification(loadNotification: LoadNotification) {
        val cacheType = decodeLoadType(loadNotification.type)
        val key = loadNotification.key
        when (loadNotification) {
            is LoadNotification.Completed -> {
                logger.info("Loaded cache entry of '$cacheType' for key '$key'")
            }
            is LoadNotification.Failed -> {
                logger.warn("Failed to load cache entry of '$cacheType' for key '$key' (will be retried = ${loadNotification.willBeRetried}): ${loadNotification.errorMessage}")
            }
        }
    }
}
