package com.rarible.loader.cache.internal

import com.rarible.core.loader.Loader
import com.rarible.loader.cache.CacheLoader
import com.rarible.loader.cache.CacheLoaderEvent
import com.rarible.loader.cache.CacheLoaderEventListener
import com.rarible.loader.cache.CacheType
import org.slf4j.LoggerFactory

class CacheLoaderRunner<T>(
    private val cacheType: CacheType,
    private val cacheLoader: CacheLoader<T>,
    private val cacheLoaderEventListener: CacheLoaderEventListener<T>,
    private val repository: CacheRepository
) : Loader {

    private val logger = LoggerFactory.getLogger(CacheLoaderRunner::class.java)

    override val type = encodeLoadType(cacheType)

    override suspend fun load(key: String) {
        logger.info("Loading cache value of '$cacheType' for key '$key'")
        val data = try {
            cacheLoader.load(key)
        } catch (e: Exception) {
            logger.warn("Failed to load cache value of '$cacheType' for key '$key'", e)
            cacheLoaderEventListener.onFailed(key, e)
            throw e
        }
        logger.info("Saving loaded cache value of '$cacheType' for key '$key'")
        val saveResult = repository.save(cacheType, key, data)
        val cacheLoaderEvent = CacheLoaderEvent(
            key,
            saveResult.previousData,
            saveResult.newData
        )
        cacheLoaderEventListener.onLoaded(cacheLoaderEvent)
    }
}
