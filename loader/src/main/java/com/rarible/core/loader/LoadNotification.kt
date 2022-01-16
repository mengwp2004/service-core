package com.rarible.core.loader

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.time.Instant

/**
 * Notifications sent to [LoadNotificationListener]s when corresponding tasks are
 * [completed][LoadNotification.Completed] or [failed][LoadNotification.Failed].
 */
@JsonTypeInfo(property = "type", use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes(
    JsonSubTypes.Type(name = "LOADED", value = LoadNotification.Completed::class),
    JsonSubTypes.Type(name = "ERROR", value = LoadNotification.Failed::class),
)
sealed class LoadNotification {

    abstract val type: LoadType

    abstract val key: String

    /**
     * The load task identified by [type] and [key], which was scheduled at [scheduledAt],
     * has completed successfully after N [attempts].
     */
    data class Completed(
        override val type: LoadType,
        override val key: String,
        val attempts: Int,
        val scheduledAt: Instant
    ) : LoadNotification()

    /**
     * Load task identified by [type] and [key] has failed with an error.
     * There were [attempts] attempts to run the task.
     * Failure message is [errorMessage].
     * The task was initially scheduled at [scheduledAt] time instant.
     * [willBeRetried] indicates whether the library will retry to run the task again.
     */
    data class Failed(
        override val type: LoadType,
        override val key: String,
        val attempts: Int,
        val scheduledAt: Instant,
        val errorMessage: String,
        val willBeRetried: Boolean
    ): LoadNotification()
}
