# Weather Stations Monitoring System

This repository contains the final project for the **Designing Data-Intensive Applications (DDIA)** course. The objective is to design and implement a complete weather monitoring system that simulates real-time data ingestion, processing, and querying using modern data infrastructure tools.

## Overview

The Internet of Things (IoT) is a major source of high-frequency data streams. Devices, or “things,” generate a constant flow of data that must be efficiently processed and analyzed. 

This project simulates a network of distributed **weather stations**, each sending real-time weather data to a **central base station** for persistence, indexing, and analytics.

## System Architecture

The system is divided into three main stages:

### 1. Data Acquisition

- Multiple weather stations act as producers.
- Each station sends periodic weather readings to a **Kafka** message broker.

### 2. Data Processing & Archiving

- A central consumer (base station) subscribes to Kafka topics.
- The data is archived in **Parquet** format for efficient storage and analytics.

### 3. Indexing & Querying

- Two indexing systems are used for fast retrieval and search:
  - **Bitcask**: A key-value store that maintains the **latest reading** from each station.
  - **Elasticsearch + Kibana**: For full-text search and real-time dashboard visualization on top of the archived Parquet files.

## Architecture Diagram

![System Architecture Diagram](https://github.com/user-attachments/assets/dddd0306-e1fa-4b5a-a859-d4df759912b4)

