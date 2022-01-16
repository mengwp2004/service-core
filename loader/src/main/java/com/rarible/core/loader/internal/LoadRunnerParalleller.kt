package com.rarible.core.loader.internal

import com.rarible.core.loader.configuration.LoadProperties
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import org.springframework.stereotype.Component
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

@Component
class LoadRunnerParalleller(
    loadProperties: LoadProperties,
    private val loadRunner: LoadRunner
) : AutoCloseable {

    private val daemonDispatcher = Executors
        .newFixedThreadPool(loadProperties.workers) { runnable ->
            Thread(runnable, "load-runner-${LOAD_RUNNER_THREAD_INDEX.getAndIncrement()}").apply {
                isDaemon = true
            }
        }
        .asCoroutineDispatcher()

    private val scope = CoroutineScope(SupervisorJob() + daemonDispatcher)

    suspend fun load(loadTasks: List<LoadTask>) {
        loadTasks.map { scope.async { loadRunner.load(it) } }.awaitAll()
    }

    override fun close() {
        daemonDispatcher.close()
    }

    private companion object {
        val LOAD_RUNNER_THREAD_INDEX = AtomicInteger()
    }
}
