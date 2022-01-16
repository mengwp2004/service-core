package com.rarible.core.loader.internal

import com.rarible.core.loader.LoadService
import com.rarible.core.loader.LoadType
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Clock

@Component
class LoadServiceImpl(
    private val loadTaskKafkaSender: LoadTaskKafkaSender,
    private val clock: Clock
) : LoadService {

    private val logger = LoggerFactory.getLogger(LoadService::class.java)

    override suspend fun scheduleLoad(loadType: LoadType, key: String) {
        val loadTask = LoadTask(
            type = loadType,
            key = key,
            attempts = 0,
            scheduledAt = clock.instant()
        )
        scheduleLoad(loadTask)
    }

    // Used from retry scheduler only, not public API.
    internal suspend fun scheduleLoad(loadTask: LoadTask) {
        logger.info("Scheduling task to run of '${loadTask.type}' for '${loadTask.key}'")
        loadTaskKafkaSender.send(loadTask)
    }

}
