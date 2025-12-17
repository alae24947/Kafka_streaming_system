# cloud project
With the rapid growth of digital applications, many systems are required to process large
amounts of data in real time. Examples include online shopping platforms, social media,
Internet of Things (IoT) devices, and system logs. In such environments, data is generated
continuously and must be transmitted, processed, and stored efficiently without delays.
Streaming platforms such as Apache Kafka are widely used to handle real-time data
flows. Kafka allows producers to publish data streams and consumers to process them
asynchronously, ensuring high throughput, scalability, and reliability. When combined
with processing components and databases, Kafka enables the creation of complete realtime data pipelines.
The objective of this project is to design and implement a real-time streaming system
based on Kafka. The system simulates continuous e-commerce order data, processes the
data in real time by applying filtering and aggregation operations, stores the processed
information in a database, and finally visualizes the results using graphical tools. The
entire infrastructure is deployed using Docker Compose to ensure reproducibility and ease
of deployment.
In simple terms, this project simulates online orders, sends them through Kafka, filters the
expensive ones, stores them in a database, and shows the results in a graph.
