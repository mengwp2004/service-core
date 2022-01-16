package com.rarible.loader.cache

data class CacheLoaderEvent<T>(
    val key: String,
    val previousData: T?,
    val newData: T
)
