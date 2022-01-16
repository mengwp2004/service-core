package com.rarible.loader.cache.internal

import com.rarible.core.loader.LoadService
import com.rarible.loader.cache.CacheLoaderService
import com.rarible.loader.cache.CacheType

class CacheLoaderServiceImpl<T>(
    override val type: CacheType,
    private val cacheRepository: CacheRepository,
    private val loadService: LoadService
) : CacheLoaderService<T> {

    override suspend fun update(key: String) {
        loadService.scheduleLoad(
            loadType = encodeLoadType(type),
            key = key
        )
    }

    override suspend fun getAvailable(key: String): T? =
        cacheRepository.get<T>(type, key)

    override suspend fun remove(key: String) {
        cacheRepository.remove(type, key)
    }
}
