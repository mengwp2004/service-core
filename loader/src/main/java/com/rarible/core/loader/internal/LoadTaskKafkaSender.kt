package com.rarible.core.loader.internal

import com.rarible.core.kafka.KafkaMessage
import com.rarible.core.kafka.RaribleKafkaProducer
import org.slf4j.LoggerFactory

class LoadTaskKafkaSender(
    private val kafkaSenders: Map<String, RaribleKafkaProducer<LoadTask>>
) {
    private val logger = LoggerFactory.getLogger(LoadTaskKafkaSender::class.java)

    suspend fun send(loadTask: LoadTask) {
        val kafkaSender = kafkaSenders[loadTask.type]
        if (kafkaSender == null) {
            logger.warn("No loader found for ${loadTask.type}")
            return
        }
        kafkaSender.send(KafkaMessage(key = loadTask.key, value = loadTask)).ensureSuccess()
    }
}
