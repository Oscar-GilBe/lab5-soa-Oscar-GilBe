[![Build Status](../../actions/workflows/CI.yml/badge.svg)](../../actions/workflows/CI.yml)

# Web Engineering 2025-2026 / Lab 5: Integration and SOA

A Spring Boot + Kotlin starter demonstrating Enterprise Integration Patterns using Spring Integration. Complete the tasks in `docs/GUIDE.md` to analyze the starter code, create EIP diagrams, and implement the correct integration flow.

## Tech stack

- Spring Boot 3.5.3
- Spring Integration
- Kotlin 2.2.10
- Java 21 (toolchain)
- Gradle 8.5

## Prerequisites

- Java 21
- Git

## Quick start

```bash
./gradlew clean build
./gradlew bootRun
# Application will start and begin processing messages
```

## Project structure

- `src/main/kotlin/soa/CronOddEvenDemo.kt`: Spring Integration flows (starter code with intentional issues)
- `src/main/resources/application.yml`: Application configuration
- `docs/GUIDE.md`: Assignment instructions with detailed guidance
- `docs/EIP.png`: Target EIP diagram showing the correct implementation

## Assignment overview

This lab teaches Enterprise Integration Patterns through hands-on debugging and implementation:

1. **Analyze**: Study the starter code to understand its current (flawed) behavior
2. **Diagram**: Create an EIP diagram documenting the starter configuration
3. **Compare**: Study the target diagram to identify differences
4. **Implement**: Fix the code to match the target architecture

See `docs/GUIDE.md` for detailed instructions.

## Learning objectives

- Understand Enterprise Integration Patterns (EIP)
- Apply Spring Integration DSL in Kotlin
- Design and document message-driven architectures
- Debug integration flows using EIP diagrams

## Code quality and formatting

```bash
./gradlew ktlintFormat ktlintCheck
```

## Bonus opportunities

Be the first to complete **at least two** of the following tasks to earn a bonus:

### 1. **Content Enricher Pattern** ✅

- **Description**: Implement a Content Enricher that adds additional data to messages as they flow through the system.
- **Implementation**: Add a content enricher that augments messages with metadata (timestamp, message ID, or external data lookup).
- **Goal**: Demonstrate understanding of the Content Enricher pattern for message enhancement.
- **Benefit**: Shows mastery of message enrichment patterns in integration scenarios.

### 2. **Splitter and Aggregator** ✅

- **Description**: Implement message Splitter and Aggregator patterns to process composite messages.
- **Implementation**: Split a batch of numbers into individual messages, process them separately, then aggregate results.
- **Goal**: Master composite message processing patterns.
- **Benefit**: Demonstrates understanding of parallel processing and result consolidation in integration flows.

### 3. **Dead Letter Channel** ✅

- **Description**: Implement proper error handling with a Dead Letter Channel for failed messages.
- **Implementation**: Add error handling that routes failed messages to a dead letter channel with retry logic.
- **Goal**: Implement robust error handling in integration flows.
- **Benefit**: Shows understanding of enterprise-grade error handling and recovery patterns.

### 4. **Wire Tap** ✅

- **Description**: Implement a Wire Tap to monitor messages without affecting the main flow.
- **Implementation**: Add wire taps to observe message content at key points without altering flow behavior.
- **Goal**: Enable non-intrusive monitoring of integration flows.
- **Benefit**: Demonstrates understanding of observability patterns in message-driven systems.

### 5. **Message History**

- **Description**: Track message history as messages flow through the integration system.
- **Implementation**: Add message history tracking that records each component a message passes through.
- **Goal**: Enable message flow traceability for debugging and auditing.
- **Benefit**: Shows understanding of message tracking and audit trail patterns.

### 6. **Dynamic Router**

- **Description**: Implement a Dynamic Router that can change routing rules at runtime.
- **Implementation**: Create a router with configurable rules that can be updated without restarting the application.
- **Goal**: Demonstrate runtime reconfiguration of integration flows.
- **Benefit**: Shows mastery of adaptive integration patterns and dynamic configuration.

### 7. **Claim Check Pattern**

- **Description**: Implement the Claim Check pattern to handle large message payloads efficiently.
- **Implementation**: Store large payloads externally and pass references through the flow, retrieving content when needed.
- **Goal**: Optimize message processing for large payloads.
- **Benefit**: Demonstrates understanding of performance optimization in integration systems.

### 8. **Idempotent Receiver**

- **Description**: Implement an Idempotent Receiver to handle duplicate messages safely.
- **Implementation**: Add idempotency support that detects and handles duplicate messages without side effects.
- **Goal**: Ensure message processing reliability in unreliable network conditions.
- **Benefit**: Shows understanding of reliable messaging patterns and duplicate detection.

### 9. **Integration Testing Framework**

- **Description**: Create a comprehensive integration testing suite for Spring Integration flows.
- **Implementation**: Use Spring Integration Test support to write tests that verify flow behavior, message routing, and transformations.
- **Goal**: Ensure integration flow correctness through automated testing.
- **Benefit**: Demonstrates testing strategies for message-driven architectures.

### 10. **Metrics and Monitoring**

- **Description**: Add comprehensive monitoring and metrics collection for integration flows.
- **Implementation**: Integrate Spring Boot Actuator and Micrometer to expose metrics about message rates, processing times, and error rates.
- **Goal**: Enable production observability of integration flows.
- **Benefit**: Shows understanding of operational concerns in integration systems.

Note: unless the goal specifies or disallows a specific framework you are free to replace the framework used in the original implementation with a different framework.
