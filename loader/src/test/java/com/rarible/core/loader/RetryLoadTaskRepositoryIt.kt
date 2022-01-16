package com.rarible.core.loader

import com.rarible.core.common.nowMillis
import com.rarible.core.loader.internal.MongoRetryLoadTaskRepository
import com.rarible.core.loader.internal.RetryLoadTask
import com.rarible.core.loader.internal.RetryLoadTaskRepository
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

class RetryLoadTaskRepositoryIt : AbstractIntegrationTest() {

    @Autowired
    lateinit var retryLoadTaskRepository: RetryLoadTaskRepository

    @Test
    fun `mongo indexes`() = runBlocking<Unit> {
        val indexInfos = mongo.indexOps(MongoRetryLoadTaskRepository.COLLECTION).indexInfo.asFlow().toSet()
        assertThat(indexInfos.map { it.name }.toSet())
            .isEqualTo(setOf("_id_", "retryAt_1__id_1"))
    }

    @Test
    fun `save and get`() = runBlocking<Unit> {
        val now = nowMillis()
        val retryTask = RetryLoadTask(
            type = "type",
            key = "key",
            attempts = 2,
            scheduledAt = now,
            retryAt = now
        )
        retryLoadTaskRepository.save(retryTask)
        assertThat(retryLoadTaskRepository.findWithMaxRetryAt(now.plusSeconds(1)).toList())
            .isEqualTo(listOf(retryTask.copy(version = 1)))
    }

    @Test
    fun remove() = runBlocking<Unit> {
        val now = nowMillis()
        val retryTask = RetryLoadTask(
            type = "type",
            key = "key",
            attempts = 2,
            scheduledAt = now,
            retryAt = now
        )
        retryLoadTaskRepository.save(retryTask)
        val maxRetryAt = now.plusSeconds(1)
        assertThat(retryLoadTaskRepository.findWithMaxRetryAt(maxRetryAt).toList())
            .isEqualTo(listOf(retryTask.copy(version = 1)))
        retryLoadTaskRepository.remove(retryTask.copy(version = 1))
        assertThat(retryLoadTaskRepository.findWithMaxRetryAt(maxRetryAt).toList()).isEmpty()
    }
}
