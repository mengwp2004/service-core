package com.rarible.core.loader

import com.rarible.core.common.nowMillis
import com.rarible.core.loader.internal.FailedLoadTask
import com.rarible.core.loader.internal.FailedLoadTaskRepository
import com.rarible.core.loader.internal.MongoFailedLoadTaskRepository
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

class FailedLoadTaskRepositoryIt : AbstractIntegrationTest() {

    @Autowired
    lateinit var failedTaskRepository: FailedLoadTaskRepository

    @Test
    fun `mongo indexes`() = runBlocking<Unit> {
        val indexInfos = mongo.indexOps(MongoFailedLoadTaskRepository.COLLECTION).indexInfo.asFlow().toSet()
        assertThat(indexInfos.map { it.name }.toSet())
            .isEqualTo(setOf("_id_", "lastFailedAt_1__id_1", "type_1_key_1"))
        assertThat(indexInfos.find { it.name == "type_1_key_1" }?.isUnique).isFalse()
    }

    @Test
    fun `save and get`() = runBlocking<Unit> {
        val type = "type"
        val key = "key"
        assertThat(failedLoadTaskRepository.get(type, key)).isNull()
        val failedLoadTask = FailedLoadTask(
            type = type,
            key = key,
            attempts = 1,
            scheduledAt = nowMillis(),
            lastFailedAt = nowMillis()
        )
        failedTaskRepository.save(failedLoadTask)
        assertThat(failedLoadTaskRepository.get(type, key)).isEqualTo(failedLoadTask)
    }

    @Test
    fun `get all`() = runBlocking<Unit> {
        val type = "type"
        val key = "key"
        val failedLoadTasks = (0 until 100).map {
            FailedLoadTask(
                type = type,
                key = key,
                attempts = 1,
                scheduledAt = nowMillis(),
                lastFailedAt = nowMillis()
            )
        }
        failedLoadTasks.forEach { failedTaskRepository.save(it) }
        assertThat(failedTaskRepository.getAll().toList()).isEqualTo(failedLoadTasks.sortedBy { it.lastFailedAt })
    }

    @Test
    fun remove() = runBlocking<Unit> {
        val type = "type"
        val key = "key"
        val failedLoadTask = FailedLoadTask(
            type = type,
            key = key,
            attempts = 1,
            scheduledAt = nowMillis(),
            lastFailedAt = nowMillis()
        )
        failedTaskRepository.save(failedLoadTask)
        assertThat(failedLoadTaskRepository.get(type, key)).isEqualTo(failedLoadTask)
        failedTaskRepository.remove(type, key)
        assertThat(failedLoadTaskRepository.get(type, key)).isNull()
    }
}
