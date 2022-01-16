package com.rarible.core.loader

import com.rarible.core.common.nowMillis
import com.rarible.core.loader.configuration.LoadProperties
import com.rarible.core.loader.configuration.RetryProperties
import com.rarible.core.loader.internal.FailedLoadTaskRepository
import com.rarible.core.loader.internal.LoadNotificationKafkaSender
import com.rarible.core.loader.internal.LoadRunner
import com.rarible.core.loader.internal.LoadFatalError
import com.rarible.core.loader.internal.LoadTask
import com.rarible.core.loader.internal.LoaderCriticalCodeExecutor
import com.rarible.core.loader.internal.RetryLoadTaskRepository
import com.rarible.core.loader.test.testLoaderType
import io.mockk.coEvery
import io.mockk.coJustRun
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.time.Clock
import java.time.Instant

class LoadRunnerTest {

    private val loader = mockk<Loader> {
        every { type } returns testLoaderType
    }

    private val retryLoadTaskRepository = mockk<RetryLoadTaskRepository>()
    private val loadNotificationKafkaSender = mockk<LoadNotificationKafkaSender>()
    private val failedLoadTaskRepository = mockk<FailedLoadTaskRepository>()
    private val clock = mockk<Clock>()
    private val loadProperties = LoadProperties(
        brokerReplicaSet = "not used in test",
        retry = RetryProperties(
            backoffDelaysMillis = listOf(1000, 2000) // 2 attempts to load after 1000 and 2000 ms.
        )
    )
    private val loadRunner = LoadRunner(
        loaders = listOf(loader),
        loadProperties = loadProperties,
        retryLoadTaskRepository = retryLoadTaskRepository,
        loadNotificationKafkaSender = loadNotificationKafkaSender,
        failedLoadTaskRepository = failedLoadTaskRepository,
        clock = clock,
        loaderCriticalCodeExecutor = LoaderCriticalCodeExecutor(
            retryAttempts = 5,
            backoffBaseDelay = 1
        )
    )

    @Test
    fun `success - send notification`(): Unit = runBlocking {
        coJustRun { loader.load(any()) }
        coJustRun { loadNotificationKafkaSender.send(any()) }
        val now = nowMillis()
        every { clock.instant() } returns now
        val loadKey = "key"
        val loadTask = LoadTask(
            type = testLoaderType,
            key = loadKey,
            attempts = 0,
            scheduledAt = now
        )
        loadRunner.load(loadTask)
        coVerify(exactly = 1) {
            loadNotificationKafkaSender.send(
                LoadNotification.Completed(
                    type = testLoaderType,
                    key = loadKey,
                    attempts = 0,
                    scheduledAt = now
                )
            )
        }
    }

    @Test
    fun `simple exception from the loader - schedule for retry later`(): Unit = runBlocking {
        val exception = RuntimeException("error-message")
        coEvery { loader.load(any()) } throws exception
        val now = nowMillis()
        every { clock.instant() } returns now
        coJustRun { retryLoadTaskRepository.save(any()) }
        coJustRun { loadNotificationKafkaSender.send(any()) }
        val loadKey = "key"
        val loadTask = LoadTask(
            type = testLoaderType,
            key = loadKey,
            attempts = 0,
            scheduledAt = now
        )
        loadRunner.load(loadTask)
        coVerify(exactly = 1) {
            loadNotificationKafkaSender.send(
                LoadNotification.Failed(
                    type = testLoaderType,
                    key = loadKey,
                    errorMessage = exception.localizedMessage,
                    attempts = 0,
                    willBeRetried = true,
                    scheduledAt = now
                )
            )
        }
        coVerify(exactly = 1) {
            retryLoadTaskRepository.save(match {
                it.type == loadTask.type
                        && it.key == loadTask.key
                        && it.retryAt == now.plusMillis(1000)
            })
        }
    }

    @Test
    fun `simple exception from the loader - mark task failed if ran out of attempts`(): Unit = runBlocking {
        val exception = RuntimeException("error-message")
        coEvery { loader.load(any()) } throws exception
        val now = nowMillis()
        every { clock.instant() } returns now
        coJustRun { failedLoadTaskRepository.save(any()) }
        coJustRun { loadNotificationKafkaSender.send(any()) }
        val loadKey = "key"
        val loadTask = LoadTask(
            type = testLoaderType,
            key = loadKey,
            attempts = 2, // Already the second (of two) retry attempt.
            scheduledAt = now
        )
        loadRunner.load(loadTask)
        coVerify(exactly = 1) {
            loadNotificationKafkaSender.send(
                LoadNotification.Failed(
                    type = testLoaderType,
                    key = loadKey,
                    attempts = 2,
                    errorMessage = exception.localizedMessage,
                    willBeRetried = false,
                    scheduledAt = now
                )
            )
        }
        coVerify(exactly = 1) {
            failedLoadTaskRepository.save(match {
                it.type == loadTask.type && it.key == loadTask.key
            })
        }
    }

    @Test
    fun `simple exception from the loader - then success - unmark the failed task`() {
        val exception = RuntimeException("error-message")
        coEvery { loader.load(any()) } throws exception
        val nowTime = Instant.ofEpochSecond(12345)
        every { clock.instant() } returns nowTime
        coJustRun { failedLoadTaskRepository.save(any()) }

    }

    @Test
    fun `fatal error - if exception on sending notification to kafka`() {
        coJustRun { loader.load(any()) }
        val kafkaException = IOException()
        coEvery { loadNotificationKafkaSender.send(any()) } throws kafkaException
        val now = nowMillis()
        every { clock.instant() } returns now
        val loadKey = "key"
        val loadTask = LoadTask(
            type = testLoaderType,
            key = loadKey,
            attempts = 2, // On the second attempt.
            scheduledAt = now
        )
        Assertions.assertThatThrownBy {
            runBlocking { loadRunner.load(loadTask) }
        }
            .isInstanceOf(LoadFatalError::class.java)
            .hasCause(kafkaException)
            .hasSuppressedException(kafkaException)

        coVerify(exactly = 5) {
            loadNotificationKafkaSender.send(
                LoadNotification.Completed(
                    type = testLoaderType,
                    key = loadKey,
                    attempts = 2,
                    scheduledAt = now
                )
            )
        }
    }

    @Test
    fun `fatal error - if loader throws an Error`(): Unit = runBlocking {
        val error = OutOfMemoryError()
        coEvery { loader.load(any()) } throws error
        val now = nowMillis()
        every { clock.instant() } returns now
        val loadTask = LoadTask(
            type = testLoaderType,
            key = "key",
            attempts = 0,
            scheduledAt = now
        )
        assertThrows<OutOfMemoryError> { loadRunner.load(loadTask) }
        coVerify(exactly = 0) { retryLoadTaskRepository.save(any()) }
        coVerify(exactly = 0) { failedLoadTaskRepository.save(any()) }
    }
}
