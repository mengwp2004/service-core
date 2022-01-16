package com.rarible.loader.cache.internal

import com.rarible.core.loader.LoadService
import com.rarible.loader.cache.CacheLoader
import com.rarible.loader.cache.CacheLoaderServiceFactory
import org.springframework.stereotype.Component

@Component
class CacheLoaderServiceFactoryImpl(
    private val cacheRepository: CacheRepository,
    private val loadService: LoadService
) : CacheLoaderServiceFactory {
    override fun <T> create(cacheLoader: CacheLoader<T>) = CacheLoaderServiceImpl<T>(
        type = cacheLoader.type,
        cacheRepository = cacheRepository,
        loadService = loadService
    )
}
