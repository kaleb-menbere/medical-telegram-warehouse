# Medical Telegram Warehouse

An end-to-end data pipeline for Telegram data analytics, built for Kara Solutions.

## Project Overview

This project builds a robust data platform that generates actionable insights about Ethiopian medical businesses using data scraped from public Telegram channels.

## Architecture

The pipeline follows a modern ELT approach:

1. **Extract & Load**: Scrape data from Telegram → Store in Data Lake
2. **Transform**: Load to PostgreSQL → Transform with dbt → Star Schema
3. **Enrich**: Object detection with YOLO on images
4. **Serve**: Analytical API with FastAPI
5. **Orchestrate**: Pipeline automation with Dagster

## Project Structure
medical-telegram-warehouse/
├── data/ # Data lake storage
├── medical_warehouse/ # dbt project
├── src/ # Source code
├── api/ # FastAPI application
├── notebooks/ # Analysis notebooks
├── tests/ # Test files
└── scripts/ # Utility scripts

## Setup Instructions

1. Clone the repository:
```bash
git clone 
cd medical-telegram-warehouse