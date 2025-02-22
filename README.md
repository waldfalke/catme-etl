# Catme Excel to JSON

Catme Excel to JSON is a Python-based tool designed to convert large Excel or CSV files into JSON format efficiently. The tool supports multi-threaded processing, checkpointing, and resource monitoring to ensure smooth operation even with large datasets.

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
   git clone https://github.com/waldfalke/catme-excel-to-json.git
   cd catme-excel-to-json
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Ensure you have Python 3.8 or higher installed.

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

## Contributing

Contributions are welcome! If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/waldfalke/catme-excel-to-json/blob/main/LICENSE) file for details.

## Acknowledgments

- Thanks to the open-source community for providing tools like `pandas`, `sqlite3`, and `psutil` that make this project possible.
