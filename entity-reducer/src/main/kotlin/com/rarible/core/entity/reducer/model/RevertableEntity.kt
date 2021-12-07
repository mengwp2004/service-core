package com.rarible.core.entity.reducer.model

/**
 * Entity that supports reverting events (for example when chain is reorganized)
 */
interface RevertableEntity<Id, Event : Comparable<Event>, E : RevertableEntity<Id, Event, E>> : Identifiable<Id> {
    /**
     * These events will always be ordered in natural order
     */
    val events: List<Event>

    /**
     * Copy and create entity with new event list
     */
    fun withEvents(events: List<Event>): E
}
