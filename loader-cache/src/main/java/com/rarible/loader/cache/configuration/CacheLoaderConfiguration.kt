package com.rarible.loader.cache.configuration

import com.rarible.core.loader.configuration.EnableRaribleLoader
import com.rarible.loader.cache.CacheLoader
import com.rarible.loader.cache.CacheLoaderEventListener
import com.rarible.loader.cache.CacheLoaderService
import com.rarible.loader.cache.CacheType
import com.rarible.loader.cache.internal.CacheLoaderRunner
import com.rarible.loader.cache.internal.CacheRepository
import com.rarible.loader.cache.internal.LoggingLoaderNotificationListener
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories

@Configuration
@EnableRaribleLoader
@EnableReactiveMongoRepositories(basePackageClasses = [CacheLoaderService::class])
@ComponentScan(basePackageClasses = [CacheLoaderService::class])
@EnableConfigurationProperties(CacheLoaderProperties::class)
class CacheLoaderConfiguration {
    @Bean
    fun cacheLoaderRunner(
        cacheLoaders: List<CacheLoader<*>>,
        cacheLoaderNotificationListeners: List<CacheLoaderEventListener<*>>,
        cacheRepository: CacheRepository
    ): List<CacheLoaderRunner<*>> =
        cacheLoaders.map { cacheLoader ->
            val type = cacheLoader.type
            val eventListener = getLoaderListener(type, cacheLoaderNotificationListeners)
            createRunner<Any>(cacheLoader, eventListener, cacheRepository)
        }

    @Bean
    fun cacheLoaderNotificationsListeners(
        cacheLoaders: List<CacheLoader<*>>
    ): List<LoggingLoaderNotificationListener> =
        cacheLoaders.map { LoggingLoaderNotificationListener(it.type) }

    private fun getLoaderListener(
        type: CacheType,
        cacheLoaderNotificationListener: List<CacheLoaderEventListener<*>>
    ): CacheLoaderEventListener<*> = cacheLoaderNotificationListener.find { it.type == type }
        ?: throw AssertionError("No associated cache loader listener found for $type")

    @Suppress("UNCHECKED_CAST")
    private fun <T> createRunner(
        cacheLoader: CacheLoader<*>,
        cacheLoaderEventListener: CacheLoaderEventListener<*>,
        cacheRepository: CacheRepository
    ) = CacheLoaderRunner(
        cacheType = cacheLoader.type,
        cacheLoader = cacheLoader as CacheLoader<T>,
        cacheLoaderEventListener = cacheLoaderEventListener as CacheLoaderEventListener<T>,
        repository = cacheRepository
    )
}
