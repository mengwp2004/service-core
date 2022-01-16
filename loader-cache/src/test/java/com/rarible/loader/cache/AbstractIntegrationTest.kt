package com.rarible.loader.cache

import com.rarible.core.mongo.configuration.IncludePersistProperties
import com.rarible.core.test.containers.KafkaTestContainer
import com.rarible.core.test.ext.MongoCleanup
import com.rarible.core.test.ext.MongoTest
import com.rarible.loader.cache.configuration.EnableRaribleCacheLoader
import com.rarible.loader.cache.test.TestImage
import com.rarible.loader.cache.test.failEvents
import com.rarible.loader.cache.test.loadEvents
import com.rarible.loader.cache.test.testCacheType
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.test.context.ContextConfiguration

@MongoTest
@MongoCleanup
@SpringBootTest(
    properties = []
)
@ContextConfiguration(classes = [TestContext::class])
abstract class AbstractIntegrationTest {
    companion object {
        val kafkaTestContainer = KafkaTestContainer()
    }

    init {
        System.setProperty("kafka.hosts", kafkaTestContainer.kafkaBoostrapServers())
    }

    @Autowired
    lateinit var imageLoadService: CacheLoaderService<TestImage>

    @Autowired
    lateinit var imageLoader: CacheLoader<TestImage>

    @BeforeEach
    fun clear() {
        clearMocks(imageLoader)
        every { imageLoader.type } returns testCacheType
        loadEvents.clear()
        failEvents.clear()
    }
}

@Configuration
@EnableAutoConfiguration
@EnableRaribleCacheLoader
@IncludePersistProperties
class TestContext {
    @Bean
    @Qualifier("test.loader")
    fun testLoader(): CacheLoader<TestImage> = mockk {
        every { type } returns testCacheType
    }

    @Bean
    fun testLoadService(
        @Qualifier("test.loader") testLoader: CacheLoader<TestImage>,
        cacheLoaderServiceFactory: CacheLoaderServiceFactory
    ): CacheLoaderService<TestImage> = cacheLoaderServiceFactory.create(testLoader)
}
