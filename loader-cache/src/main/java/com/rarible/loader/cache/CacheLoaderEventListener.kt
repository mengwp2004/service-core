package com.rarible.loader.cache

interface CacheLoaderEventListener<T> {
    val type: CacheType

    suspend fun onLoaded(cacheLoaderEvent: CacheLoaderEvent<T>)

    suspend fun onFailed(key: String, exception: Exception)
}
