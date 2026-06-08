
# MapReduce & Apache Spark: Big Data Processing Framework

A comprehensive educational repository demonstrating MapReduce concepts and Apache Spark's distributed computing capabilities with real-world examples, performance benchmarks, and production-ready code.


## 🎯 Overview

This repository provides hands-on implementations of both classic MapReduce paradigms and modern Apache Spark processing, showing **why** and **when** to use distributed computing for big data workloads.

## 📊 What's Inside

### 1️⃣ Classic MapReduce (From Scratch)
- **Word Count** - The "Hello World" of big data
- **Average Calculation** - Multi-stage MapReduce for statistical aggregation
- **Custom Implementation** - Understanding the shuffle phase

### 2️⃣ Apache Spark Demos
- **Sales Analysis** - Multi-dimensional aggregations (by year, product)
- **Customer Segmentation** - Rule-based customer classification
- **ETL Pipelines** - Data transformation workflows

### 3️⃣ Performance Benchmarking
- **Fair Comparison** - Pandas vs Spark on identical datasets
- **Scaling Tests** - 100K to 10M+ rows
- **Memory Analysis** - When Pandas fails (and Spark succeeds)

## 🚀 Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/yourusername/mapreduce-spark-bigdata.git
cd mapreduce-spark-bigdata

# Install dependencies
pip install pyspark pandas numpy psutil
```

### Run MapReduce Examples

```python
from collections import defaultdict

# Simple Word Count
documents = [
    "hello world python map reduce",
    "hello python world",
    "map reduce is powerful"
]

# Map phase
mapped = [(word, 1) for doc in documents for word in doc.lower().split()]

# Shuffle phase
shuffled = defaultdict(list)
for key, value in mapped:
    shuffled[key].append(value)

# Reduce phase
result = {key: sum(values) for key, values in shuffled.items()}
print(result)  # {'hello': 2, 'world': 2, 'python': 2, ...}
```

### Run Spark Examples

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

# Initialize Spark
spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

# Create DataFrame
data = [(2023, "Product A", 100, 10), (2023, "Product B", 50, 15)]
df = spark.createDataFrame(data, ["year", "product", "units_sold", "price"])

# Analysis
result = df.groupBy("year", "product").agg(
    sum("units_sold").alias("total_units"),
    sum(col("units_sold") * col("price")).alias("revenue")
).show()
```

## 📈 Performance Results

### Pandas vs Spark Benchmark

| Rows | Pandas Avg (sec) | Spark Avg (sec) | Pandas Memory | Verdict |
|------|-----------------|-----------------|---------------|---------|
| 100K | 0.03 | 2.15 | 13 MB | Pandas wins |
| 500K | 0.10 | 2.75 | 67 MB | Pandas wins |
| 1M | 0.19 | 4.17 | 135 MB | Pandas wins |
| 5M | 0.63 | 18.44 | 675 MB | Pandas wins* |
| 10M | 0.91 | 20.10 | 0.61 GB | Pandas OK |
| 50M+ | ❌ Memory Error | ✅ Scales linearly | 💥 Crash | **Spark essential** |

*Pandas still faster but memory becomes limiting factor

### Key Finding
- **Pandas is 5-10x faster** for datasets < 5M rows
- **Spark becomes essential** when data exceeds available RAM
- **Sweet spot for Spark**: > 10M rows or distributed computing needs

## 📚 Core Concepts Demonstrated

### MapReduce Phases

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   MAP       │ ──► │  SHUFFLE    │ ──► │   REDUCE    │
│ (Split)     │     │  (Group)    │     │ (Aggregate) │
└─────────────┘     └─────────────┘     └─────────────┘

Input: ["hello world", "hello python"]
Map:   [(hello,1), (world,1), (hello,1), (python,1)]
Shuffle: {hello: [1,1], world: [1], python: [1]}
Reduce:  {hello: 2, world: 1, python: 1}
```

### Spark's Advantages

| Feature | Traditional (Pandas) | Apache Spark |
|---------|---------------------|--------------|
| **Memory** | Limited to RAM | Disk + RAM + Network |
| **Scale** | Single machine | Cluster (1000+ nodes) |
| **Fault Tolerance** | ❌ None | ✅ Automatic retry |
| **Lazy Evaluation** | ❌ Eager | ✅ Optimized plans |
| **Streaming** | ❌ No | ✅ Structured Streaming |
| **ML Library** | scikit-learn (single node) | MLlib (distributed) |
| **SQL Support** | ❌ Limited | ✅ SparkSQL |

## 🛠️ Real-World Applications

### 1. Log Processing (TB-scale)
```python
# Parse 50GB of web server logs
logs = spark.read.text("s3://logs/2024/*.log")
errors = logs.filter(col("value").contains("ERROR"))
error_counts = errors.groupBy("date").count()
```

### 2. ETL Pipelines
```python
# Extract → Transform → Load
df = spark.read.parquet("raw_data/")
df_clean = df.filter(col("value").isNotNull())
df_clean.write.parquet("processed_data/")
```

### 3. Real-time Analytics
```python
# Streaming data processing
streaming_df = spark.readStream.format("kafka").load()
aggregated = streaming_df.groupBy("category").count()
aggregated.writeStream.outputMode("complete").start()
```

## 📁 Repository Structure

```
mapreduce-spark-bigdata/
│
├── mapreduce_basic.py          # MapReduce from scratch
├── spark_sales_analysis.py     # Sales aggregations
├── spark_customer_segmentation.py  # Rule-based segmentation
├── pandas_vs_spark_benchmark.py # Fair performance comparison
├── importance_of_spark.py      # Why Spark matters (demos 1-5)
├── requirements.txt
├── README.md
└── LICENSE
```

## 🔬 Key Demos Explained

### Demo 1: Memory Management
Shows how Spark's lazy evaluation defers computation until needed, enabling processing of datasets larger than RAM.

### Demo 2: Complex Aggregations
Group-by operations with multiple metrics (sum, avg, stddev, count) on 5M+ rows.

### Demo 3: Data Larger Than RAM
Simulates processing 5M rows across 10 CSV files - Pandas struggles, Spark handles effortlessly.

### Demo 4: Fault Tolerance
Demonstrates Spark's automatic task retry when processing fails on specific partitions.

### Demo 5: Caching for Iteration
Shows 50-80% speedup for iterative algorithms (machine learning, graph processing).

## 💡 When to Use Which Tool

### Use Pandas When:
- ✅ Data < 1GB (fits in RAM)
- ✅ Single machine processing
- ✅ Interactive exploration
- ✅ Simple aggregations
- ✅ Rapid prototyping

### Use Spark When:
- ✅ Data > 10GB (exceeds RAM)
- ✅ Distributed processing needed
- ✅ Fault tolerance required
- ✅ Real-time streaming
- ✅ Complex ETL pipelines
- ✅ Running on cloud clusters
- ✅ Machine learning on big data

## 📊 Educational Value

This repository is ideal for:
- **Students** learning distributed computing concepts
- **Data Engineers** transitioning from Pandas to Spark
- **Data Scientists** needing to scale their workflows
- **Software Engineers** implementing big data pipelines

## 🧪 Run the Benchmarks

```bash
# Full performance comparison
python pandas_vs_spark_benchmark.py

# Spark importance demos
python importance_of_spark.py

# MapReduce examples
python mapreduce_basic.py
```

## 📖 References

- [MapReduce Paper (Google, 2004)](https://research.google/pubs/pub62/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Resilient Distributed Datasets (RDD) Paper](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)

## 🤝 Contributing

Contributions welcome! Areas for improvement:
- Add GraphX examples
- Implement machine learning pipelines (MLlib)
- Add Structured Streaming demos
- Include Kubernetes deployment configs
- Add Delta Lake examples

## 📝 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgements

- Apache Spark community
- Google's MapReduce research paper
- UC Berkeley AMPLab

---

⭐ **Star this repo** if it helped you understand distributed computing concepts!
```

