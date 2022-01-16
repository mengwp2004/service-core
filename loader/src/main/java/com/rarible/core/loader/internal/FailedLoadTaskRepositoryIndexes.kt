package com.rarible.core.loader.internal

import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.index.Index

object FailedLoadTaskRepositoryIndexes {

    private val logger = LoggerFactory.getLogger(FailedLoadTaskRepositoryIndexes::class.java)

    suspend fun ensureIndexes(mongo: ReactiveMongoOperations) {
        val collection = MongoFailedLoadTaskRepository.COLLECTION
        logger.info("Ensuring Mongo indexes on $collection")
        val indexOps = mongo.indexOps(collection)
        val indexes = listOf(
            Index()
                .on(FailedLoadTask::lastFailedAt.name, Sort.Direction.ASC)
                .on("_id", Sort.Direction.ASC),

            Index()
                .on(FailedLoadTask::type.name, Sort.Direction.ASC)
                .on(FailedLoadTask::key.name, Sort.Direction.ASC)
        )
        indexes.forEach { index ->
            logger.info("Ensuring Mongo index ${index.indexKeys.keys} on $collection")
            indexOps.ensureIndex(index).awaitFirst()
        }
    }
}
