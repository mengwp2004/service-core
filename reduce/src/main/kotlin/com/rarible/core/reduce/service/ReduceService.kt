package com.rarible.core.reduce.service

import com.rarible.core.common.retryOptimisticLock
import com.rarible.core.reduce.model.DataKey
import com.rarible.core.reduce.repository.DataRepository
import com.rarible.core.reduce.model.ReduceEvent
import com.rarible.core.reduce.model.ReduceSnapshot
import com.rarible.core.reduce.repository.ReduceEventRepository
import com.rarible.core.reduce.repository.SnapshotRepository
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import java.util.*

class ReduceService<in Event : ReduceEvent<Mark>, Mark : Comparable<Mark>, Data, Key : DataKey>(
    private val reducer: Reducer<Event, Mark, Data, Key>,
    private val eventRepository: ReduceEventRepository<Event, Mark, Key>,
    private val snapshotRepository: SnapshotRepository<Mark, Data, Key>,
    private val dataRepository: DataRepository<Data>,
    private val minEventsBeforeNexSnapshot: Long
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun onEvents(events: List<Event>) {
        if (events.isEmpty()) return

        val context = ReduceContext(events)

        events.toFlux()
            .map { event -> reducer.getDataKeyFromEvent(event) }
            .distinct()
            .flatMap { update(it, context) }
            .awaitFirstOrNull()
    }

    private fun update(key: Key?, context: ReduceContext<Event, Mark>) = mono {
        val snapshot = key
            ?.let { snapshotRepository.get(it) }
            ?.takeIf { context.minMark > it.mark }

        eventRepository.getEvents(key, snapshot?.mark)
            .windowUntilChanged { event -> reducer.getDataKeyFromEvent(event) }
            .concatMap {
                it.switchOnFirst { first, events ->
                    val firstEvent = first.get()

                    if (firstEvent != null) {
                        val targetKey = reducer.getDataKeyFromEvent(firstEvent)
                        logger.info("Started processing $targetKey")

                        val initial = snapshot?.data ?: reducer.getInitialData(targetKey)

                        updateData(initial, events)
                            .retryOptimisticLock()
                            .thenReturn(targetKey)
                    } else {
                        Mono.empty()
                    }
                }
            }
            .then()
    }

    private fun updateData(initialData: Data, events: Flux<Event>) = mono {
        val stack = Stack<ReduceSnapshot<Data, Mark>>()

        val reducedData = events
            .window(minEventsBeforeNexSnapshot.toInt())
            .asFlow()
            .fold(initialData) { initial, window ->
                val intermediateSnapshot = reducer.reduce(initial, window.asFlow())
                stack.push(intermediateSnapshot)

                intermediateSnapshot.data
            }

        if (reducedData != initialData) {
            dataRepository.save(reducedData)

            val nextSnapshot = stack.lastOrNull()
            val needSaveSnapshot = stack.size == 3

            if (nextSnapshot != null && needSaveSnapshot) {
                snapshotRepository.save(nextSnapshot)
            }
        }
    }

    private class ReduceContext<out Event : ReduceEvent<Mark>, Mark : Comparable<Mark>>(
        events: List<Event>
    ) {
        val minMark: Mark = events.minBy { it.mark }?.mark ?: error("Events array can't be empty")
    }
}