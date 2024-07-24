# Qurious

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status][actions-badge]][actions-url]

[actions-badge]: https://github.com/holicc/qurious/actions/workflows/rust.yml/badge.svg
[actions-url]: https://github.com/holicc/qurious/actions?query=branch%3Amain

## Description

> Yet another SQL query engine. inspired by Apache DataFusion.

Qurious is a high-performance, in-memory query engine written in Rust and built on top of the Apache Arrow framework. It offers a powerful and familiar SQL interface, similar to Apache DataFusion, allowing you to efficiently analyze large datasets stored in various formats. By leveraging the strengths of Arrow, Qurious provides efficient memory management and seamless data exchange with other Arrow-based tools.

## Development Status

It's important to emphasize that Qurious is in its early development phase. While the team is actively working on it, the project is not yet ready for production use. The features and functionalities outlined above are still under development and subject to change as progress is made.

## Key Features

- SQL Compatibility: Qurious supports a wide range of SQL functionalities, making it easy to learn and use for those familiar with SQL.
- High Performance: Benefiting from Rust's efficiency and memory safety, Qurious provides fast query execution times on large datasets, enabling you to gain insights quickly.
- In-Memory Processing: Data is processed within the system's memory for blazing-fast performance, making it ideal for real-time analytics or scenarios demanding rapid response times.
- Supported Data Formats: Qurious can read and write data from diverse formats, including CSV, Parquet, JSON, Avro, and potentially more (depending on your implementation).
- Integration with Arrow ecosystem: Qurious seamlessly integrates with other Arrow-based tools and libraries, enabling smooth data exchange and workflow optimization.
