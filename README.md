# Real-time-Trade-Event-for-Cryptocurrencies
## Project Original Thoughts:
Have you been confused by the volatility of the cryptocurrency market and struggled to make sense of its rapid price fluctuations? 

By using real-time data from CoinCap API via websockets, we are able to stream the latest trading information for Bitcoin and Ethereum and process it using Spark structured streaming in order to create a dashboard that visualizes the data in an easily digestible format.

## Project Overview:
This application is developed with a live streaming dataset obtained from the CoinCap API using websockets. The goal of this project is to build a real-time data pipeline using Apache Spark's Structured Streaming module, which will process and analyze the incoming data, and store the results in a MySQL database. Additionally, we will be using Tableau to create a dashboard that displays the processed data in an easily digestible format.

## Project Technical Overview:
The project can be broken down into the following components:
1.	Data Ingestion: The CoinCap API provides live cryptocurrency trade data through websockets.
2.	Data Distribution: Used Apache Kafka as a message broker to receive and distribute the data stream from the CoinCap API. The incoming data will be formatted in JSON, which will be used to feed the Spark Structured Streaming application.
3.	Data Processing: Processed and analyzed the incoming data from Kafka in real-time. use the Structured Streaming module to create a streaming application that processes the incoming data, aggregates it in 1-minute tumbling windows.
4.	Data display: stored the processed data in a Delta table.

## Project Approach:
1.	Define Kafka configurations.
2.	Subscribe to the WebSocket stream from CoinCap.io and send the trade events to a Kafka topic.
3.	Read the trade events from the Kafka topic using PySpark.
4.	Perform a sliding window aggregation to calculate the total trade volume, total trade amount, and average price per minute for a given sliding window of 1 minute.
5.	Calculate the average price trend over the last 6 minutes using a PySpark DataFrame.
6.	Display the resulting PySpark DataFrame in the console output.
7.	Repeat steps 4-6 every minute for a total of 10 times.

## Kafka topic configuration
```python
admin_client = KafkaAdminClient(
    bootstrap_servers = ${bootstrap_servers}
    request_timeout_ms = 30000
)
```

## Kafka producer configuration
```Python
kafka_brokers = ${bootstrap_servers} 
producer = KafkaProducer(bootstrap_servers=kafka_brokers, api_version=(2, 8, 1))
topic = ${Kafka_topic}
```

This project has the potential to benefit both individual traders and businesses involved in cryptocurrency trading by providing them with real-time insights into market trends and fluctuations.
