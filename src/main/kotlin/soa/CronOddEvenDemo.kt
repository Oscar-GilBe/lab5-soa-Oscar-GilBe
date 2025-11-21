@file:Suppress("WildcardImport", "NoWildcardImports", "MagicNumber")

package soa

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Lazy
import org.springframework.integration.annotation.Gateway
import org.springframework.integration.annotation.MessagingGateway
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.integration.channel.QueueChannel
import org.springframework.integration.config.EnableIntegration
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.MessageChannels
import org.springframework.integration.dsl.Pollers
import org.springframework.integration.dsl.PublishSubscribeChannelSpec
import org.springframework.integration.dsl.integrationFlow
import org.springframework.integration.handler.advice.RequestHandlerRetryAdvice
import org.springframework.integration.support.MessageBuilder
import org.springframework.messaging.Message
import org.springframework.messaging.MessageChannel
import org.springframework.retry.backoff.ExponentialBackOffPolicy
import org.springframework.retry.policy.SimpleRetryPolicy
import org.springframework.retry.support.RetryTemplate
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

private val logger = LoggerFactory.getLogger("soa.CronOddEvenDemo")

/**
 * Spring Integration configuration for demonstrating Enterprise Integration Patterns.
 * This application implements a message flow that processes numbers and routes them
 * based on whether they are even or odd.
 *
 * **Your Task**: Analyze this configuration, create an EIP diagram, and compare it
 * with the target diagram to identify and fix any issues.
 */
@SpringBootApplication
@EnableIntegration
@EnableScheduling
class IntegrationApplication(
    private val sendNumber: SendNumber,
    private val sendBatch: SendBatch,
    private val sendRiskyNumber: SendRiskyNumber,
) {
    @Autowired
    @Qualifier("deadLetterChannel")
    @Lazy
    lateinit var dlq: MessageChannel

    /**
     * Creates an atomic integer source that generates sequential numbers.
     */
    @Bean
    fun integerSource(): AtomicInteger = AtomicInteger()

    /**
     * Defines a publish-subscribe channel for odd numbers.
     * Multiple subscribers can receive messages from this channel.
     */
    @Bean
    fun oddChannel(): PublishSubscribeChannelSpec<*> = MessageChannels.publishSubscribe()

    /**
     * Creates an executor channel for parallel processing of split messages.
     * Uses virtual threads (lightweight threads) for concurrent message processing.
     */
    @Bean
    fun splitNumberChannel() = MessageChannels.executor(Executors.newVirtualThreadPerTaskExecutor())

    /**
     * Dead Letter Channel for storing messages that fail after all retry attempts.
     */
    @Bean
    fun deadLetterChannel(): MessageChannel = QueueChannel()

    /**
     * Retry advice configuration with exponential backoff policy.
     * Retries up to 3 times with exponential delay between attempts.
     */
    @Bean
    fun retryAdvice(): RequestHandlerRetryAdvice {
        val retryTemplate =
            RetryTemplate().apply {
                setRetryPolicy(SimpleRetryPolicy().apply { maxAttempts = 3 })
                setBackOffPolicy(
                    ExponentialBackOffPolicy().apply {
                        initialInterval = 400
                        multiplier = 1.5
                        maxInterval = 5000
                    },
                )
            }

        return RequestHandlerRetryAdvice().apply {
            setRetryTemplate(retryTemplate)
            setRecoveryCallback { context ->
                val failedMessage = context.getAttribute("message") as? Message<*>
                val errorMessage =
                    MessageBuilder
                        .withPayload(failedMessage?.payload ?: "UNKNOWN")
                        .setHeader("error", context.lastThrowable?.message)
                        .setHeader("exceptionType", context.lastThrowable?.javaClass?.name)
                        .setHeader("errorTimestamp", System.currentTimeMillis())
                        .setHeader("failedMessage", failedMessage)
                        .build()
                dlq.send(errorMessage)
                logger.error("Retry exhausted for message: {}, sending to DLQ", failedMessage?.payload)
                errorMessage
            }
        }
    }

    /**
     * Source flow that polls the integer source and sends to numberChannel.
     * Polls every 100ms.
     */
    @Bean
    fun myFlow(integerSource: AtomicInteger): IntegrationFlow =
        integrationFlow(
            source = { integerSource.getAndIncrement() },
            options = { poller(Pollers.fixedRate(100)) },
        ) {
            // Content Enricher, add metadata to messages
            enrichHeaders {
                header("messageSource", "PollingSource")
                header("messageType", "SequentialNumber")
            }
            transform { num: Int ->
                logger.info("📥 Source generated number: {}", num)
                num
            }
            wireTap("tapChannel")
            channel("numberChannel")
        }

    /**
     * Routing flow that receives from numberChannel and routes based on even/odd logic.
     */
    @Bean
    fun numberFlow(): IntegrationFlow =
        integrationFlow("numberChannel") {
            // Content Enricher, add routing context
            enrichHeaders {
                headerExpression("routingTimestamp", "T(System).currentTimeMillis()")
                header("routerName", "EvenOddRouter")
            }
            wireTap("tapChannel")
            route { p: Int ->
                val channel = if (p % 2 == 0) "evenChannel" else "oddChannel"
                logger.info("🔀 Router: {} → {}", p, channel)
                channel
            }
        }

    /**
     * Integration flow for processing even numbers.
     * Transforms integers to strings and logs the result.
     */
    @Bean
    fun evenFlow(): IntegrationFlow =
        integrationFlow("evenChannel") {
            // Content Enricher, add processing metadata
            enrichHeaders {
                header("parity", "EVEN")
                header("processingPath", "EvenFlow")
                headerExpression("processingStartTime", "T(System).currentTimeMillis()")
            }
            transform { obj: Int ->
                logger.info("  ⚙️  Even Transformer: {} → 'Number {}'", obj, obj)
                "Number $obj"
            }
            // Enrich with transformation completion time
            enrichHeaders {
                headerExpression("transformationCompletedAt", "T(System).currentTimeMillis()")
            }
            wireTap("tapChannel")
            handle { p ->
                val startTime = p.headers["processingStartTime"] as? Long
                val endTime = p.headers["transformationCompletedAt"] as? Long
                val duration = if (startTime != null && endTime != null) endTime - startTime else 0
                logger.info("  ✅ Even Handler: Processed [{}] (duration: {}ms)", p.payload, duration)
            }
        }

    /**
     * Integration flow for processing odd numbers.
     * Applies a filter before transformation and logging.
     */
    @Bean
    fun oddFlow(): IntegrationFlow =
        integrationFlow("oddChannel") {
            // Content Enricher, add processing metadata
            enrichHeaders {
                header("parity", "ODD")
                header("processingPath", "OddFlow")
                headerExpression("processingStartTime", "T(System).currentTimeMillis()")
            }
            filter { p: Int ->
                val passes = p % 2 != 0
                // Note: this filter always pass all numbers because p is always odd here. This filter could be removed.
                logger.info("  🔍 Odd Filter: checking {} → {}", p, if (passes) "PASS" else "REJECT")
                passes
            }
            // Enrich with filter result
            enrichHeaders {
                header("filterPassed", true)
                headerExpression("filterCompletedAt", "T(System).currentTimeMillis()")
            }
            transform { obj: Int ->
                logger.info("  ⚙️  Odd Transformer: {} → 'Number {}'", obj, obj)
                "Number $obj"
            }
            // Enrich with transformation completion time
            enrichHeaders {
                headerExpression("transformationCompletedAt", "T(System).currentTimeMillis()")
            }
            wireTap("tapChannel")
            handle { p ->
                val startTime = p.headers["processingStartTime"] as? Long
                val endTime = p.headers["transformationCompletedAt"] as? Long
                val duration = if (startTime != null && endTime != null) endTime - startTime else 0
                logger.info("  ✅ Odd Handler: Processed [{}] (duration: {}ms)", p.payload, duration)
            }
        }

    /**
     * Scheduled task that periodically sends negative random numbers via the gateway.
     */
    @Scheduled(fixedRate = 1000)
    fun sendNumber() {
        val number = -Random.nextInt(100)
        logger.info("🚀 Gateway injecting: {}", number)
        sendNumber.sendNumber(number)
    }

    // ========== Splitter and Aggregator Pattern ==========

    /**
     * Scheduled task that sends batches of numbers every 5 seconds after an initial delay of 2 seconds.
     */
    @Scheduled(fixedRate = 5000, initialDelay = 2000)
    fun sendBatchNumbers() {
        val batch = List(5) { Random.nextInt(1, 100) }
        logger.info("Batch Gateway: Sending batch of {} numbers: {}", batch.size, batch)
        sendBatch.sendBatch(batch)
    }

    /**
     * Splitter flow: Splits a batch of numbers into individual messages.
     * Each number is processed independently in parallel.
     * Implements the Splitter EIP pattern.
     */
    @Bean
    fun batchSplitterFlow(): IntegrationFlow =
        integrationFlow("batchChannel") {
            // Content Enricher, add batch metadata
            enrichHeaders {
                header("batchSource", "BatchGateway")
                headerExpression("batchReceivedAt", "T(System).currentTimeMillis()")
                headerFunction<Int>("batchSize") { message ->
                    (message.payload as? List<*>)?.size ?: 0
                }
            }
            wireTap("tapChannel")
            // Each number in the batch is split into its own message
            split()
            // Enrich each split message
            enrichHeaders {
                header("splitFrom", "BatchSplitter")
            }
            transform { num: Int ->
                logger.info("Splitter: Split out number {}", num)
                num
            }
            wireTap("tapChannel")
            channel("splitNumberChannel")
        }

    /**
     * Process each split number independently in parallel using virtual threads.
     * Squares each number to demonstrate concurrent processing.
     * Simulates some processing time to show parallelism.
     */
    @Bean
    fun processSplitNumbersFlow(): IntegrationFlow =
        integrationFlow("splitNumberChannel") {
            transform { num: Int ->
                // Simulate some computation time to demonstrate parallel processing
                val startTime = System.currentTimeMillis()
                Thread.sleep(100) // Simulated processing delay
                val squared = num * num
                val processingTime = System.currentTimeMillis() - startTime
                logger.info(
                    "Processor: $num squared = $squared " +
                        "[Time: ${processingTime}ms]",
                )
                squared
            }
            channel("aggregateChannel")
        }

    /**
     * Aggregator flow: Collects all processed numbers and consolidates them.
     * Waits until all split messages are received, then aggregates the results.
     * Implements the Aggregator EIP pattern.
     */
    @Bean
    fun aggregatorFlow(): IntegrationFlow =
        integrationFlow("aggregateChannel") {
            // Spring Integration will automatically group messages that belong together based on correlation.
            aggregate()
            transform { payload: Any ->
                val numbers = (payload as? List<*>)?.mapNotNull { it as? Int } ?: emptyList()
                val sum = numbers.sum()
                val avg = if (numbers.isNotEmpty()) sum / numbers.size else 0
                logger.info("Aggregator: Collected {} squared values, sum={}, avg={}", numbers.size, sum, avg)
                mapOf(
                    "count" to numbers.size,
                    "values" to numbers,
                    "sum" to sum,
                    "average" to avg,
                )
            }
            handle { p ->
                logger.info("Batch Result Handler: Final aggregated result = {}", p.payload)
            }
        }

    // ========== Retry and Dead Letter Pattern ==========

    /**
     * Scheduled task that sends risky numbers every 3 seconds to test error handling.
     */
    @Scheduled(fixedRate = 3000, initialDelay = 4000)
    fun sendRiskyNumbers() {
        val riskyNumbers = listOf(0, 13, 666, 7, 42, 99)
        val number = riskyNumbers.random()
        logger.info("Risky Gateway: Sending number {} for processing", number)
        sendRiskyNumber.sendRiskyNumber(number)
    }

    /**
     * Risky number processing flow with retry advice.
     */
    @Bean
    fun riskyIngressFlow(
        retryAdvice: RequestHandlerRetryAdvice,
        handler: RiskyNumberHandler,
    ): IntegrationFlow =
        integrationFlow("riskyIngressChannel") {
            handle<Int>(
                { payload, headers ->
                    // Only handler execution is subject to retries
                    handler.processRiskyNumber(payload)
                },
            ) {
                // If processRiskyNumber throws exception, retryAdvice triggers RetryTemplate and retries as configured
                // After exhausting attempts, retryAdvice executes the recoveryCallback
                // defined above (which will send to the DLQ)
                advice(retryAdvice)
            }
        }

    /**
     * Dead Letter Queue flow for handling failed messages.
     */
    @Bean
    fun deadLetterFlow(): IntegrationFlow =
        integrationFlow {
            channel("deadLetterChannel")
            handle { msg ->
                val failedMessage = msg.headers["failedMessage"] as? Message<*>
                logger.error("DLQ: Received failed message - Payload: {}", failedMessage?.payload)
                logger.error("Error: {}", msg.headers["error"])
                logger.error("All retries exhausted, message permanently failed")
            }
        }

    /**
     * Handler bean for processing risky numbers.
     */
    @Bean
    fun riskyNumberHandler() = RiskyNumberHandler()

    // ========== Wire Tap Pattern ==========

    /**
     * Wire Tap monitoring flow for observing messages without affecting the main flow.
     * Implements the Wire Tap EIP pattern for non-intrusive monitoring.
     * Shows enriched headers added by Content Enricher pattern.
     */
    @Bean
    fun wireTapFlow(): IntegrationFlow =
        integrationFlow {
            channel("tapChannel")
            handle { msg ->
                val payload = msg.payload
                val payloadType = payload?.javaClass?.simpleName ?: "Unknown"
                logger.info("Wire Tap: Monitoring message. Payload: {} (Type: {})", payload, payloadType)

                // Show enriched headers (Content Enricher metadata)
                val enrichedHeaders =
                    msg.headers.filterKeys {
                        // Exclude standard headers to focus on custom ones
                        it !in listOf("id", "timestamp", "replyChannel", "errorChannel")
                    }
                if (enrichedHeaders.isNotEmpty()) {
                    logger.info("Wire Tap: Enriched Headers: {}", enrichedHeaders)
                }
            }
        }
}

/**
 * Service component that processes messages from the odd channel.
 * Uses @ServiceActivator annotation to connect to the integration flow.
 */
@Component
class SomeService {
    @ServiceActivator(inputChannel = "oddChannel")
    fun handle(p: Any) {
        logger.info("  🔧 Service Activator: Received [{}] (type: {})", p, p.javaClass.simpleName)
    }
}

/**
 * Messaging Gateway for sending numbers into the integration flow.
 * This provides a simple interface to inject messages into the system.
 */
@MessagingGateway
interface SendNumber {
    @Gateway(requestChannel = "numberChannel")
    fun sendNumber(number: Int)
}

/**
 * Messaging Gateway for sending batches of numbers.
 * Used to demonstrate the Splitter and Aggregator pattern.
 */
@MessagingGateway
interface SendBatch {
    @Gateway(requestChannel = "batchChannel")
    fun sendBatch(numbers: List<Int>)
}

/**
 * Messaging Gateway for sending risky numbers that may fail.
 */
@MessagingGateway
interface SendRiskyNumber {
    @Gateway(requestChannel = "riskyIngressChannel")
    fun sendRiskyNumber(number: Int)
}

/**
 * Handler component for processing risky numbers with potential failures.
 */
@Component
class RiskyNumberHandler {
    fun processRiskyNumber(payload: Int): Int {
        logger.info("Handler: Processing number {}", payload)

        return when (payload) {
            0 -> {
                logger.error("ERROR Handler: Cannot process zero")
                throw IllegalArgumentException("Zero is not allowed")
            }
            13 -> {
                logger.error("ERROR Handler: Unlucky number 13")
                throw IllegalStateException("Unlucky number")
            }
            666 -> {
                logger.error("ERROR Handler: Invalid number 666")
                throw RuntimeException("Invalid number")
            }
            else -> {
                logger.info("Handler: Successfully processed number {}", payload)
                payload
            }
        }
    }
}

fun main() {
    runApplication<IntegrationApplication>()
}
