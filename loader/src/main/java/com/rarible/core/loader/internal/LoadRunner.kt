package com.rarible.core.loader.internal

import com.rarible.core.loader.LoadNotification
import com.rarible.core.loader.Loader
import com.rarible.core.loader.configuration.LoadProperties
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Clock

@Component
class LoadRunner(
    loaders: List<Loader>,
    private val loadProperties: LoadProperties,
    private val retryLoadTaskRepository: RetryLoadTaskRepository,
    private val loadNotificationKafkaSender: LoadNotificationKafkaSender,
    private val failedLoadTaskRepository: FailedLoadTaskRepository,
    private val clock: Clock,
    private val loaderCriticalCodeExecutor: LoaderCriticalCodeExecutor
) {
    private val logger = LoggerFactory.getLogger(LoadRunner::class.java)

    init {
        loaders.map { it.type }.groupingBy { it }.eachCount().forEach { (key, count) ->
            check(count == 1) { "Loader $key is duplicated" }
        }
    }

    private val loaders = loaders.associateBy { it.type }

    suspend fun load(loadTask: LoadTask) {
        val loader = loaders[loadTask.type]
        if (loader == null) {
            logger.warn("No load executor found for ${loadTask.type}")
            return
        }
        try {
            loader.load(loadTask.key)
        } catch (e: Throwable) {
            if (e is Error) {
                logger.error("Fatal failure to load $loadTask", e)
                throw e
            }
            logger.info("Failed to load $loadTask", e)
            val willBeRetried = loadTask.attempts < loadProperties.retry.retryAttempts
            if (willBeRetried) {
                loaderCriticalCodeExecutor.retryOrFatal("Schedule retry of $loadTask") {
                    scheduleRetry(loadTask)
                }
            } else {
                loaderCriticalCodeExecutor.retryOrFatal("Mark task failed $loadTask") {
                    markFailed(loadTask)
                }
            }
            loaderCriticalCodeExecutor.retryOrFatal("Send failure notification for $loadTask") {
                val errorMessage = e.localizedMessage ?: e.message ?: e::class.java.simpleName
                loadNotificationKafkaSender.send(
                    LoadNotification.Failed(
                        type = loadTask.type,
                        key = loadTask.key,
                        attempts = loadTask.attempts,
                        scheduledAt = loadTask.scheduledAt,
                        errorMessage = errorMessage,
                        willBeRetried = willBeRetried
                    )
                )
            }
            return
        }
        logger.info("Loaded successfully $loadTask")
        loaderCriticalCodeExecutor.retryOrFatal("Send successful notification for $loadTask") {
            loadNotificationKafkaSender.send(
                LoadNotification.Completed(
                    type = loadTask.type,
                    key = loadTask.key,
                    attempts = loadTask.attempts,
                    scheduledAt = loadTask.scheduledAt
                )
            )
        }
    }

    private suspend fun scheduleRetry(loadTask: LoadTask) {
        val retryAt = clock.nowMillis() + loadProperties.retry.getRetryDelay(loadTask.attempts)
        val retryLoadTask = RetryLoadTask(
            type = loadTask.type,
            key = loadTask.key,
            attempts = loadTask.attempts + 1,
            scheduledAt = loadTask.scheduledAt,
            retryAt = retryAt
        )
        retryLoadTaskRepository.save(retryLoadTask)
        logger.info("Scheduled task for retry later: $retryLoadTask")
    }

    private suspend fun markFailed(loadTask: LoadTask) {
        val failedLoadTask = FailedLoadTask(
            type = loadTask.type,
            key = loadTask.key,
            attempts = loadTask.attempts,
            scheduledAt = loadTask.scheduledAt,
            lastFailedAt = clock.nowMillis()
        )
        failedLoadTaskRepository.save(failedLoadTask)
        logger.info("Task has run out of ${loadProperties.retry.retryAttempts} attempts and was marked failed: $loadTask")
    }
}
