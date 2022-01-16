package com.rarible.loader.cache

interface CacheLoaderServiceFactory {
    fun <T> create(cacheLoader: CacheLoader<T>): CacheLoaderService<T>
}
