package com.rarible.core.reduce.model

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import kotlin.Comparator

class ReduceException(message : String) : IllegalStateException(message)

interface Mark : Comparable<Mark>

interface MarkService<L : Log<L>, R : LogRecord<L, R>> {
    fun get(logRecord: R): Mark

    fun isStableMark(mark: Mark): Boolean
}

interface RecordMapper<L : Log<L>, R : LogRecord<L, R>> {
    suspend fun map(logRecord: R): Flow<R>
}

interface Entity<K, L : Log<L>, R : LogRecord<L, R>, E : Entity<K, L, R, E>> {
    val id: K

    val logRecords: List<R>

    fun withLogRecords(records: List<R>): E
}

interface EntityService<K, L : Log<L>, R : LogRecord<L, R>, E : Entity<K, L, R, E>> {
    suspend fun get(id: K): E?

    suspend fun update(entity: E): E

    fun getEntityTemplate(id: K): E

    fun getEntityId(logRecord: R): K
}

interface Reducer<K, L : Log<L>, R : LogRecord<L, R>, E : Entity<K, L, R, E>> {
    suspend fun reduce(entity: E, record: R): E
}

interface ReduceService<K, L : Log<L>, R : LogRecord<L, R>, E : Entity<K, L, R, E>> {
    suspend fun reduce(logRecords: Flow<R>)
}

class RecordList<L : Log<L>, R : LogRecord<L, R>>(
    records: List<R>,
    private val markService: MarkService<L, R>
) {
    private val comparator: Comparator<R> = Comparator.comparing { record -> record.mark }
    private val records = records.sortedWith(comparator.reversed())
    private val latestMark = records.firstOrNull()?.mark

    fun canBeApplied(record: R): Boolean {
        return when (record.log?.status) {
            Log.Status.CONFIRMED, Log.Status.PENDING -> recordAddIndex(record) >= 0
            Log.Status.REVERTED, Log.Status.DROPPED, Log.Status.INACTIVE -> recordRemoveIndex(record) >= 0
            null -> throw ReduceException("Can't get log from record $record")
        }
    }

    fun addOrRemove(record: R): List<R> {
        val newRecords = records.toMutableList()

        when (record.log?.status) {
            Log.Status.CONFIRMED, Log.Status.PENDING -> {
                val index = recordAddIndex(record)
                requireIndex(index >= 0) { "Can't add record $record" }
                newRecords.add(index, record)
            }
            Log.Status.REVERTED, Log.Status.DROPPED, Log.Status.INACTIVE -> {
                val index = recordRemoveIndex(record)
                requireIndex(index > 0) { "Can't remove record $record" }
                newRecords.removeAt(index)
            }
            null -> throw ReduceException("Can't get log from record $record")
        }
        return newRecords.filter { markService.isStableMark(it.mark).not() }
    }

    private fun recordAddIndex(record: R): Int {
        return if (latestMark == null || record.mark > latestMark) 0 else -records.binarySearch(record, comparator.reversed())
    }

    private fun recordRemoveIndex(record: R): Int {
        return if (record.mark == latestMark) 0 else records.binarySearch(record, comparator.reversed())
    }

    private fun requireIndex(value: Boolean, message: () -> Any) {
        if (!value) {
            throw ReduceException(message.toString())
        }
    }

    private val R.mark: Mark
        get() = markService.get(this)
}

@FlowPreview
open class EntityReduceService<K, L : Log<L>, R : LogRecord<L, R>, E : Entity<K, L, R, E>>(
    private val markService: MarkService<L, R>,
    private val recordMapper: RecordMapper<L, R>,
    private val entityService: EntityService<K, L, R, E>,
    private val reducer: Reducer<K, L, R, E>
) : ReduceService<K, L, R, E> {

    override suspend fun reduce(logRecords: Flow<R>) {
        logRecords
            .flatMapConcat { record -> recordMapper.map(record) }
            .collect { record ->
                val entityKey = entityService.getEntityId(record)
                val currentEntity = entityService.get(entityKey) ?: entityService.getEntityTemplate(entityKey)
                val records = RecordList(currentEntity.logRecords, markService)

                if (records.canBeApplied(record)) {
                    val updatedRecords = records.addOrRemove(record)
                    val updatedEntity = reducer.reduce(currentEntity, record).withLogRecords(updatedRecords)
                    entityService.update(updatedEntity)
                }
        }
    }
}

