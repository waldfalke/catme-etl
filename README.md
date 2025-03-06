# Catme ETL

Catme ETL is a Python-based tool designed to convert large Excel or CSV files into JSON format efficiently. The tool supports multi-threaded processing, checkpointing, and resource monitoring to ensure smooth operation even with large datasets.

## Features

- **Multi-format Support**: Convert both Excel (`.xlsx`, `.xls`) and CSV files into JSON.
- **Chunk Processing**: Process data in chunks to handle large files without memory issues.
- **Checkpointing**: Save progress using SQLite checkpoints to resume interrupted tasks.
- **Resource Monitoring**: Monitor CPU and memory usage to prevent system overload.
- **Parallel Execution**: Utilize multiple CPU cores for faster processing.
- **Error Handling**: Graceful handling of errors and interruptions, ensuring no data loss.
- **Logging**: Comprehensive logging for debugging and monitoring.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/waldfalke/catme-etl.git
   cd catme-etl
   ```

2. Install the required dependencies manually (we intentionally do not include a `requirements.txt` file):
   - **Python 3.8 or higher** must be installed on your system.
   - Install the following Python packages using `pip`:
     ```bash
     pip install pandas openpyxl tqdm psutil sqlite3
     ```
     - `pandas`: For reading and processing Excel/CSV files.
     - `openpyxl`: To support `.xlsx` files.
     - `tqdm`: For progress bars during processing.
     - `psutil`: For system resource monitoring.
     - `sqlite3`: For checkpointing and saving progress.

3. Ensure that you have sufficient disk space for temporary files and logs.

## Usage

### Basic Command-Line Usage

```bash
python unified_code.py <input_file> <output_file> --format <excel|csv> --chunk-size <size> --workers <num_workers> --csv-separator <separator>
```

#### Arguments:

- `input_file`: Path to the input file (Excel or CSV).
- `output_file`: Path to the output JSON file.
- `--format`: Specify the input file format (`excel` or `csv`). Default is `excel`.
- `--chunk-size`: Number of rows to process per chunk. Default is `500`.
- `--workers`: Number of worker processes for parallel execution. Default is the number of CPU cores minus one.
- `--csv-separator`: Separator for CSV files. Default is `,`.

#### Example:

Convert an Excel file to JSON with a chunk size of 1000 and 4 worker processes:

```bash
python unified_code.py Input/example.xlsx Output/output.json --format excel --chunk-size 1000 --workers 4
```

### Directory Structure

- **Input/**: Place your input files here.
- **Output/**: Converted JSON files and checkpoints will be saved here.
- **logs/**: Log files for monitoring and debugging.
- **WIP/**: Temporary working directory for intermediate files.

### Resuming Interrupted Tasks

If the process is interrupted, it can be resumed from the last checkpoint. Simply rerun the same command, and the tool will pick up where it left off.

## Configuration

- **Chunk Size**: Adjust the `--chunk-size` parameter based on your system's memory capacity.
- **Workers**: Use the `--workers` parameter to control the level of parallelism. A good starting point is the number of CPU cores minus one.

## Logging

Logs are stored in the `logs/` directory. Each process generates its own log file with a unique identifier (PID). Logs include timestamps, process IDs, and detailed messages for debugging.

## Principles of AI-Compatible Development

This program adheres to several key principles of AI-compatible development to ensure scalability, maintainability, and ease of integration with AI systems:

1. **Explicit Context Management**:
   - All functions and methods explicitly pass their dependencies (e.g., file paths, configurations) rather than relying on implicit global state. This makes the code easier to analyze and debug by AI systems.

2. **Stateless Design**:
   - Core processing functions (e.g., `process_chunk_function`) are stateless and self-contained. They create their own instances of dependencies (e.g., readers) instead of relying on shared state. This eliminates serialization issues and ensures compatibility with multiprocessing frameworks.

3. **Modularity and Separation of Concerns**:
   - The code is divided into clear modules, each responsible for a specific task (e.g., reading files, writing results, monitoring resources). This separation simplifies maintenance and allows AI tools to focus on specific parts of the codebase.

4. **Error Handling and Graceful Degradation**:
   - The program is designed to handle interruptions gracefully, ensuring no data loss. Checkpointing and logging mechanisms allow the system to recover from failures and provide detailed diagnostics for troubleshooting.

5. **Scalability and Parallelism**:
   - The use of `ProcessPoolExecutor` and chunk-based processing ensures that the program can scale to handle large datasets efficiently. Resource monitoring prevents overloading the system, making it suitable for deployment in diverse environments.

6. **Human-Readable Documentation**:
   - The code includes comprehensive inline documentation and type annotations, making it easier for both humans and AI systems to understand the purpose and behavior of each component.

7. **Avoidance of Implicit Dependencies**:
   - The program avoids reliance on external configuration files like `requirements.txt`, allowing users to manually install dependencies. This approach ensures transparency and flexibility in dependency management.

## Contributing

Contributions are welcome! If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/waldfalke/catme-excel-to-json/blob/main/LICENSE) file for details.

## Acknowledgments

- Thanks to the open-source community for providing tools like `pandas`, `sqlite3`, and `psutil` that make this project possible.
