package com.rarible.core.loader.internal

import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.index.Index

object RetryLoadTaskRepositoryIndexes {

    private val logger = LoggerFactory.getLogger(RetryLoadTaskRepositoryIndexes::class.java)

    suspend fun ensureIndexes(mongo: ReactiveMongoOperations) {
        val collection = MongoRetryLoadTaskRepository.COLLECTION
        logger.info("Ensuring Mongo indexes on $collection")
        val retryIndexOps = mongo.indexOps(collection)
        val indexes = listOf(
            Index()
                .on(RetryLoadTask::retryAt.name, Sort.Direction.ASC)
                .on("_id", Sort.Direction.ASC)
        )
        indexes.forEach { index ->
            logger.info("Ensuring Mongo index ${index.indexKeys.keys} on $collection")
            retryIndexOps.ensureIndex(index).awaitFirst()
        }
    }
}
