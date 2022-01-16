package com.rarible.core.loader.internal

import com.rarible.core.loader.LoadType
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.find
import org.springframework.data.mongodb.core.findOne
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component

// TODO[loader]: remove failed tasks from the repository when the task completes without error.
interface FailedLoadTaskRepository {
    fun getAll(): Flow<FailedLoadTask>
    suspend fun get(type: LoadType, key: String): FailedLoadTask?
    suspend fun save(task: FailedLoadTask)
    suspend fun remove(type: LoadType, key: String)
}

@Component
class MongoFailedLoadTaskRepository(
    private val mongo: ReactiveMongoOperations
) : FailedLoadTaskRepository {

    companion object {
        const val COLLECTION = "loader-tasks-failed"
    }

    override fun getAll(): Flow<FailedLoadTask> {
        val query = Query().apply {
            with(Sort.by(FailedLoadTask::lastFailedAt.name, "_id"))
        }
        return mongo.find<FailedLoadTask>(query, COLLECTION).asFlow()
    }

    override suspend fun get(type: LoadType, key: String): FailedLoadTask? {
        val query = Query(
            (FailedLoadTask::type isEqualTo type)
                .andOperator(FailedLoadTask::key isEqualTo key)
        )
        return mongo.findOne<FailedLoadTask>(query, COLLECTION).awaitSingleOrNull()
    }

    override suspend fun save(task: FailedLoadTask) {
        mongo.save(task, COLLECTION).awaitFirst()
    }

    override suspend fun remove(type: LoadType, key: String) {
        val query = Query(
            (FailedLoadTask::type isEqualTo type)
                .andOperator(FailedLoadTask::key isEqualTo key)
        )
        mongo.remove(query, COLLECTION).awaitSingleOrNull()
    }
}
