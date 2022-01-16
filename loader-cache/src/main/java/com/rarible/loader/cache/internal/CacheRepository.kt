package com.rarible.loader.cache.internal

import com.rarible.core.common.nowMillis
import com.rarible.core.common.optimisticLock
import com.rarible.loader.cache.CacheType
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.findById
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component

@Component
class CacheRepository(
    private val mongo: ReactiveMongoOperations
) {

    suspend fun <T> save(type: CacheType, key: String, data: T): CacheRepositorySaveResult<T> {
        val cacheEntry = CacheEntry(
            key = key,
            data = data as Any,
            cachedAt = nowMillis()
        )
        val (previousData, newData) = optimisticLock {
            val previousEntry = getCacheEntry(type, key)
            mongo.save(
                cacheEntry.copy(version = previousEntry?.version ?: cacheEntry.version),
                getCacheCollection(type)
            ).awaitSingle()
            @Suppress("UNCHECKED_CAST")
            previousEntry?.data as? T to data
        }
        return CacheRepositorySaveResult(key, previousData, newData)
    }

    suspend fun <T> get(type: CacheType, key: String): T? {
        @Suppress("UNCHECKED_CAST")
        return getCacheEntry(type, key)?.data as? T
    }

    private suspend fun getCacheEntry(type: CacheType, key: String) =
        mongo.findById<CacheEntry>(key, getCacheCollection(type))
            .awaitSingleOrNull()

    suspend fun remove(type: CacheType, key: String) {
        val query = Query(Criteria("_id").isEqualTo(key))
        mongo.remove(query, getCacheCollection(type)).awaitSingleOrNull()
    }

    private fun getCacheCollection(cacheType: CacheType): String = "cache-$cacheType"
}
