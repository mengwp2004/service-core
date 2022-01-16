package com.rarible.loader.cache.test

import com.rarible.loader.cache.CacheLoaderEvent
import com.rarible.loader.cache.CacheLoaderEventListener
import org.springframework.stereotype.Component
import java.util.concurrent.CopyOnWriteArrayList

const val testCacheType = "test-image"

val loadEvents: CopyOnWriteArrayList<CacheLoaderEvent<TestImage>> = CopyOnWriteArrayList()
val failEvents: CopyOnWriteArrayList<Pair<String, Exception>> = CopyOnWriteArrayList()

@Component
class TestCacheLoaderEventListener : CacheLoaderEventListener<TestImage> {
    override val type = testCacheType

    override suspend fun onLoaded(cacheLoaderEvent: CacheLoaderEvent<TestImage>) {
        loadEvents += cacheLoaderEvent
    }

    override suspend fun onFailed(key: String, exception: Exception) {
        failEvents += key to exception
    }
}
