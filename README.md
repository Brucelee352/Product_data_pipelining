---
title: "Sourcing Product Data as JSON, for Containerized Data Ingestion"
description: "This article provides a step-by-step guide on how to source product data as JSON, for containerized data ingestion. It covers the process of generating fake data, saving it as a JSON file, and using it for testing and development purposes."
date: 2025-01-07
author: "Bruce"
tags: ["data", "minio", "python", "docker", "data-sourcing", "data-ingestion"]
---

# Introduction

This project serves as a guide on how to produce business-centric product data as JSON, for containerized data ingestion; and further orchestration using Docker, MinIO and Python. I'm looking to make this as simple as possible, while being applicable to real-world business scenarios and use-cases.

# Data Sourcing

The central piece of this project is the Python script that produces the data itself. I'm specifically generating columns that reflect how real-world users interact with transactional databases; relying on the following assumptions:

- Their data has comprehensive metadata, including the date and time of the transaction, the user's ID, the product's ID, and the price of the product.
