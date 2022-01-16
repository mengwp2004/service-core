package com.rarible.loader.cache

import com.rarible.core.test.wait.Wait
import com.rarible.loader.cache.test.TestImage
import com.rarible.loader.cache.test.failEvents
import com.rarible.loader.cache.test.loadEvents
import io.mockk.ManyAnswersAnswer
import io.mockk.ThrowingAnswer
import io.mockk.coEvery
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CacheLoaderIt : AbstractIntegrationTest() {
    @Test
    fun `load simple`() = runBlocking<Unit> {
        val key = "image"
        val testImage = TestImage("content")
        coEvery { imageLoader.load(key) } returns testImage
        assertThat(imageLoadService.getAvailable(key)).isNull()
        imageLoadService.update(key)
        Wait.waitAssert {
            assertThat(loadEvents).containsExactly(
                CacheLoaderEvent(
                    key = key,
                    previousData = null,
                    newData = testImage
                )
            )
        }
        assertThat(imageLoadService.getAvailable(key)).isEqualTo(testImage)
    }

    @Test
    fun update() = runBlocking<Unit> {
        val key = "image"
        val testImage = TestImage("content")
        coEvery { imageLoader.load(key) } returns testImage
        imageLoadService.update(key)
        Wait.waitAssert { assertThat(loadEvents).hasSize(1) }

        val testImage2 = TestImage("content2")
        coEvery { imageLoader.load(key) } returns testImage2
        imageLoadService.update(key)

        Wait.waitAssert {
            assertThat(loadEvents).hasSize(2)
            assertThat(loadEvents.last()).isEqualTo(
                CacheLoaderEvent(
                    key = key,
                    previousData = testImage,
                    newData = testImage2
                )
            )
        }
        assertThat(imageLoadService.getAvailable(key)).isEqualTo(testImage2)
    }

    @Test
    fun `retry multiple times if an error occurs`() = runBlocking<Unit> {
        val key = "image"
        val error = RuntimeException("error")
        val testImage = TestImage("content")
        coEvery { imageLoader.load(key) } throws (error) andThenThrows (error) andThen testImage
        assertThat(imageLoadService.getAvailable(key)).isNull()
        imageLoadService.update(key)
        Wait.waitAssert {
            assertThat(failEvents).containsExactly(
                key to error,
                key to error
            )
            assertThat(loadEvents).containsExactly(
                CacheLoaderEvent(
                    key = key,
                    previousData = null,
                    newData = testImage
                )
            )
        }
        assertThat(imageLoadService.getAvailable(key)).isEqualTo(testImage)
    }

    @Test
    fun remove() = runBlocking<Unit> {
        val key = "image"
        val testImage = TestImage("content")
        coEvery { imageLoader.load(key) } returns testImage
        imageLoadService.update(key)
        Wait.waitAssert { assertThat(loadEvents).hasSize(1) }
        assertThat(imageLoadService.getAvailable(key)).isNotNull
        imageLoadService.remove(key)
        assertThat(imageLoadService.getAvailable(key)).isNull()
    }
}
