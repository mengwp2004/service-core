package com.rarible.core.loader

import com.rarible.core.common.nowMillis
import com.rarible.core.loader.internal.FailedLoadTask
import com.rarible.core.loader.test.testLoaderType
import com.rarible.core.loader.test.testReceivedNotifications
import com.rarible.core.test.data.randomLong
import com.rarible.core.test.data.randomString
import com.rarible.core.test.wait.BlockingWait
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

class LoadIt : AbstractIntegrationTest() {

    @Autowired
    lateinit var loadService: LoadService

    @Test
    fun `load and receive notification`() {
        val loadKey = randomString()
        coEvery { loader.load(loadKey) } coAnswers {
            delay(randomLong(100, 3000))
        }
        val now = nowMillis()
        every { clock.instant() } returns now
        runBlocking { loadService.scheduleLoad(testLoaderType, loadKey) }
        BlockingWait.waitAssert {
            coVerify(exactly = 1) { loader.load(loadKey) }
            assertThat(testReceivedNotifications).containsExactly(
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
    fun `retry on error`() {
        val loadKey = randomString()
        val exception = RuntimeException("test")
        coEvery { loader.load(loadKey) } throws (exception) coAndThen { }
        val now = nowMillis()
        every { clock.instant() } returns now coAndThen { nowMillis() }
        runBlocking { loadService.scheduleLoad(testLoaderType, loadKey) }
        BlockingWait.waitAssert {
            coVerify(exactly = 2) { loader.load(loadKey) }
            assertThat(testReceivedNotifications).containsExactly(
                LoadNotification.Failed(
                    type = testLoaderType,
                    key = loadKey,
                    attempts = 0,
                    errorMessage = exception.localizedMessage,
                    willBeRetried = true,
                    scheduledAt = now
                ),
                LoadNotification.Completed(
                    type = testLoaderType,
                    key = loadKey,
                    attempts = 1,
                    scheduledAt = now
                )
            )
        }
    }

    @Test
    fun `retry 5 times after 100-200-300-400-500 ms then mark failed`() {
        val loadKey = randomString()
        val exception = RuntimeException("test")
        val loadInstants = CopyOnWriteArrayList<Instant>()
        coEvery { loader.load(loadKey) } answers {
            loadInstants += nowMillis()
            throw exception
        }
        val now = nowMillis()
        every { clock.instant() } returns (now) coAndThen { nowMillis() }
        runBlocking { loadService.scheduleLoad(testLoaderType, loadKey) }
        BlockingWait.waitAssert(timeout = 10000) {
            coVerify(exactly = 6) { loader.load(loadKey) }
            assertThat(loadInstants).hasSize(6)
            assertThat(loadProperties.retry.backoffDelaysMillis).hasSize(5)
            // Each load attempt must have been delayed with an increasing retry strategy.
            loadInstants.windowed(size = 2).forEachIndexed { index, (thisCallInstant, nextCallInstant) ->
                assertThat(Duration.between(thisCallInstant, nextCallInstant))
                    .isGreaterThan(loadProperties.retry.getRetryDelay(index))
                    .withFailMessage { loadInstants.joinToString() }
            }
            val failedNotification = LoadNotification.Failed(
                type = testLoaderType,
                key = loadKey,
                attempts = 0,
                errorMessage = exception.localizedMessage,
                willBeRetried = true,
                scheduledAt = now
            )
            assertThat(testReceivedNotifications).containsExactly(
                failedNotification,
                failedNotification.copy(attempts = 1),
                failedNotification.copy(attempts = 2),
                failedNotification.copy(attempts = 3),
                failedNotification.copy(attempts = 4),
                failedNotification.copy(attempts = 5, willBeRetried = false)
            )
            runBlocking {
                assertThat(failedLoadTaskRepository.getAll().toList()).anySatisfy { failedTask ->
                    assertThat(failedTask).isEqualTo(
                        FailedLoadTask(
                            type = testLoaderType,
                            key = loadKey,
                            attempts = 5,
                            scheduledAt = now,

                            lastFailedAt = failedTask.lastFailedAt,
                            id = failedTask.id
                        )
                    )
                }
            }
        }
    }
}
