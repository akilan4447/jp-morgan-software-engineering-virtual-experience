# Midas Core - JPMorgan Chase Software Engineering Virtual Experience

This repository contains my completed implementation of **Midas Core**, a backend transaction processing system built as part of the JPMorgan Chase & Co. Advanced Software Engineering Job Simulation.

##  Project Overview

The goal of this project was to extend a Spring Boot microservice to handle streaming financial transactions, store them in a database, and integrate with an external API to calculate and apply incentives.

### Key Features Implemented:
* **Message Consumption (Kafka):** Implemented a Kafka listener to consume streaming transaction data from an Apache Kafka topic.
* **Database Integration (H2 & Spring Data JPA):** Designed data models and configured an embedded H2 Database to persist validated transactions and update user account balances.
* **External API Integration (REST):** Utilized Spring's `RestTemplate` to interact with an external REST API (Incentive API) to fetch dynamic incentives and apply them to transaction recipients.
* **RESTful Endpoints:** Developed a robust REST Controller (`/balance`) to allow external clients to securely query user balances.
* **Integration Testing:** Configured embedded Kafka and H2 testing environments to ensure end-to-end component functionality without external dependencies.

##  Tech Stack

* **Java 17**
* **Spring Boot** (Web, Data JPA, Kafka)
* **Apache Kafka**
* **H2 Embedded Database**
* **JUnit & Mockito** (Testing)

##  What I Learned

* **Event-Driven Architecture:** Gained hands-on experience handling continuous data streams using Kafka.
* **Component Integration:** Learned how to seamlessly connect database repositories, REST controllers, and messaging listeners within the Spring ecosystem.
* **API Contracts:** Understood how REST APIs act as strict contracts between microservices, ensuring independent deployment and robust error handling.
* **Testing Best Practices:** Configured isolated integration tests for asynchronous Kafka producers/listeners and embedded databases.
