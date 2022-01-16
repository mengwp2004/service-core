package com.rarible.core.loader.internal

import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock

@Component
class RetryTasksRunnerJob(
    private val loadService: LoadServiceImpl,
    private val retryLoadTaskRepository: RetryLoadTaskRepository,
    private val clock: Clock
) {

    private val logger = LoggerFactory.getLogger(RetryTasksRunnerJob::class.java)

    @Scheduled(
        initialDelayString = "\${loader.retry.job.runner.period:30000}",
        fixedDelayString = "\${loader.retry.job.runner.period:30000}"
    )
    fun retryTasks() {
        val logPrefix = "Retrying to load tasks"
        logger.info("$logPrefix: searching for the ready to run tasks")
        val retryTasks = retryLoadTaskRepository.findWithMaxRetryAt(maxRetryAt = clock.instant())
        val numberOfTasks = runBlocking {
            retryTasks
                .onEach {
                    scheduleTask(it, logPrefix)
                    retryLoadTaskRepository.remove(it)
                }
                .count()
        }
        logger.info("$logPrefix: $numberOfTasks failed tasks found")
    }

    private suspend fun scheduleTask(retryTask: RetryLoadTask, logPrefix: String) {
        val loadTask = LoadTask(
            type = retryTask.type,
            key = retryTask.key,
            attempts = retryTask.attempts,
            scheduledAt = retryTask.scheduledAt
        )
        logger.info("$logPrefix: scheduling retry task to run $retryTask")
        loadService.scheduleLoad(loadTask)
    }
}
