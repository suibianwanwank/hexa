# Hexa
<img src="https://img.shields.io/badge/Made%20with-JAVA%20%26%20C%2B%2B-red" alt="JAVA&RUST"><br>
Hexa is an open source OLAP database that supports cross-origin queries by connecting data instead of ETL <br>

Learn more: [Blog to Hexa]()

## Features

* **Cross-origin query:** Supports registration of connections to multiple data sources and execution of cross-source queries
* **Vectorized query engine:** A vectorized execution engine based on datafusion.
* **Optimization:** For complex queries can be optimized based on rbo and cbo. Specific performance improvements can be seen in the performance of TPC-DS benchmark
* **SQL synatx:** Support for easy-to-use SQL DDL syntax and traditional SQL query syntax.
* **Cli:** Smart synatx hints, highlighting keyword.


## Demo
show some basic syntax and cli usage
 <p align="left">
    <img src="https://suibianwanwan.oss-cn-hangzhou.aliyuncs.com/hexa.gif">
   </a>
</p>

## Getting Started
See [Guide](docs/guide.md)


## Architecture Overview

 <p align="left">
    <img src="https://suibianwanwan.oss-cn-hangzhou.aliyuncs.com/Hexa%20Architecture.png">
   </a>
</p>

Hexa internal architecture mainly consists of Cli , FrontEnd and BackEnd (the future expansion of JDBC connections and other connection methods).

FE is mainly responsible for planning and parser, and connecting to the datastore (API compatible with the KV and relational databases, currently supports rocksDb and mongoDB). Manage metadata and configuration information etc. BE takes on the role of workder and supports the connection with all the datasources.


## Acknowledgement
