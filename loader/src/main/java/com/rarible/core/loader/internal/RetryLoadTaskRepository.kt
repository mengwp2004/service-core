package com.rarible.core.loader.internal

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.find
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.lt
import org.springframework.stereotype.Component
import java.time.Instant

interface RetryLoadTaskRepository {
    suspend fun save(task: RetryLoadTask): RetryLoadTask

    fun findWithMaxRetryAt(maxRetryAt: Instant): Flow<RetryLoadTask>

    suspend fun remove(task: RetryLoadTask)
}

@Component
class MongoRetryLoadTaskRepository(
    private val mongo: ReactiveMongoOperations
) : RetryLoadTaskRepository {

    companion object {
        const val COLLECTION = "loader-tasks-retry"
    }

    override suspend fun save(task: RetryLoadTask): RetryLoadTask =
        mongo.save(task, COLLECTION).awaitFirst()

    override fun findWithMaxRetryAt(maxRetryAt: Instant): Flow<RetryLoadTask> {
        val query = Query(RetryLoadTask::retryAt lt maxRetryAt)
            .with(Sort.by(Sort.Direction.ASC, RetryLoadTask::retryAt.name, "_id"))
        return mongo.find<RetryLoadTask>(query, COLLECTION).asFlow()
    }

    override suspend fun remove(task: RetryLoadTask) {
        mongo.remove(task, COLLECTION).awaitFirstOrNull()
    }
}
