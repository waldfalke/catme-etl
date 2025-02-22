# unified_code.py

import os
import sys
import json
import logging
import platform
from pathlib import Path
import pandas as pd
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from concurrent.futures import ProcessPoolExecutor, as_completed
import psutil
from abc import ABC, abstractmethod
import sqlite3
import threading
from datetime import datetime
from tqdm import tqdm
import argparse
import shutil


# === Конфигурация директорий ===
INPUT_DIR = Path("Input")
WIP_DIR = Path("WIP")
OUTPUT_DIR = Path("Output")

for dir_path in [INPUT_DIR, WIP_DIR, OUTPUT_DIR]:
    dir_path.mkdir(exist_ok=True)


# === Очистка папки WIP ===
def clear_wip_directory():
    for file in WIP_DIR.iterdir():
        if file.is_file():
            file.unlink()
        elif file.is_dir():
            shutil.rmtree(file)


# === Очистка чекпоинтов ===
def clear_checkpoints(checkpoint_db: Path):
    """Очищает базу данных чекпоинтов."""
    if checkpoint_db.exists():
        try:
            checkpoint_db.unlink()
        except PermissionError as e:
            print(f"Ошибка при удалении файла {checkpoint_db}: {e}")


# === Конфигурация логгера ===
class MultiProcessLogger:
    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        self.log_dir.mkdir(exist_ok=True)
        self._lock = threading.Lock()

    def get_logger(self, name: str) -> logging.Logger:
        logger = logging.getLogger(name)
        if not logger.handlers:
            with self._lock:
                pid = os.getpid()
                log_file = self.log_dir / f"{name}_{pid}.log"

                handler = logging.FileHandler(log_file, encoding="utf-8")
                handler.setFormatter(
                    logging.Formatter("%(asctime)s [%(levelname)s] [PID:%(process)d] %(message)s")
                )
                logger.addHandler(handler)
                logger.setLevel(logging.INFO)

        return logger


# === Системный мониторинг ===
class SystemMonitor:
    def __init__(self, threshold_cpu: float = 90.0, threshold_memory: float = 90.0):
        self.threshold_cpu = threshold_cpu
        self.threshold_memory = threshold_memory
        self._start_time = datetime.now()

    def check_resources(self) -> dict:
        cpu_percent = psutil.cpu_percent()
        memory_percent = psutil.virtual_memory().percent

        return {
            "cpu_usage": cpu_percent,
            "memory_usage": memory_percent,
            "runtime": (datetime.now() - self._start_time).total_seconds(),
            "is_critical": cpu_percent > self.threshold_cpu or memory_percent > self.threshold_memory
        }


# === SQLite-based чекпоинты ===
class SQLiteCheckpoint:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn = sqlite3.connect(str(self.db_path))
        self._init_db()

    def _init_db(self):
        with self._conn:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                    sheet_name TEXT,
                    chunk_index INTEGER,
                    processed_rows INTEGER,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (sheet_name, chunk_index)
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)

    def save(self, sheet_name: str, chunk_index: int, processed_rows: int):
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO checkpoints (sheet_name, chunk_index, processed_rows) VALUES (?, ?, ?)",
                (sheet_name, chunk_index, processed_rows)
            )

    def load(self, sheet_name: str) -> Dict[int, int]:
        cursor = self._conn.execute(
            "SELECT chunk_index, processed_rows FROM checkpoints WHERE sheet_name = ?",
            (sheet_name,)
        )
        return dict(cursor.fetchall())

    def close(self):
        """Закрывает соединение с базой данных."""
        if self._conn:
            self._conn.close()


# === Интерфейс для форматов файлов ===
class DataReader(ABC):
    @abstractmethod
    def validate(self) -> bool:
        pass

    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def read_chunk(self, sheet_name: str, start: int, size: int) -> pd.DataFrame:
        pass


class ExcelReader(DataReader):
    def __init__(self, file_path: Path, **kwargs):
        self.file_path = file_path

    def validate(self) -> bool:
        try:
            pd.read_excel(self.file_path, nrows=0)
            return True
        except Exception as e:
            raise ValueError(f"Invalid Excel file: {e}")

    def get_metadata(self) -> Dict[str, Any]:
        excel = pd.ExcelFile(self.file_path)
        metadata = {}
        for sheet in excel.sheet_names:
            df = excel.parse(sheet, nrows=0)
            metadata[sheet] = {
                "rows": excel.parse(sheet).shape[0],
                "columns": df.columns.tolist(),
                "dtypes": df.dtypes.to_dict()
            }
        return metadata

    def read_chunk(self, sheet_name: str, start: int, size: int) -> pd.DataFrame:
        return pd.read_excel(
            self.file_path,
            sheet_name=sheet_name,
            skiprows=range(1, start + 1) if start > 0 else None,
            nrows=size
        )


class CSVReader(DataReader):
    def __init__(self, file_path: Path, **kwargs):
        self.file_path = file_path
        self.csv_kwargs = kwargs

    def validate(self) -> bool:
        try:
            pd.read_csv(self.file_path, nrows=0, **self.csv_kwargs)
            return True
        except Exception as e:
            raise ValueError(f"Invalid CSV file: {e}")

    def get_metadata(self) -> Dict[str, Any]:
        df = pd.read_csv(self.file_path, nrows=0, **self.csv_kwargs)
        return {
            "default": {
                "rows": sum(1 for _ in open(self.file_path)) - 1,
                "columns": df.columns.tolist(),
                "dtypes": df.dtypes.to_dict()
            }
        }

    def read_chunk(self, sheet_name: str, start: int, size: int) -> pd.DataFrame:
        return pd.read_csv(
            self.file_path,
            skiprows=range(1, start + 1) if start > 0 else None,
            nrows=size,
            **self.csv_kwargs
        )


# === Топ-уровневая функция обработки чанка ===
def process_chunk_function(reader_cls, file_path: Path, sheet_name: str, start: int, size: int, reader_kwargs: dict) -> List[dict]:
    """
    Обрабатывает указанный чанк данных, создавая новый экземпляр ридера.
    """
    reader = reader_cls(file_path, **reader_kwargs)
    df = reader.read_chunk(sheet_name, start, size)
    return df.to_dict(orient="records")


# === Класс для записи временных JSON-файлов ===
class ChunkWriter:
    def __init__(self, wip_dir: Path):
        self.wip_dir = wip_dir
        self.wip_dir.mkdir(exist_ok=True)
        self._lock = threading.Lock()

    def write_chunk(self, chunk_index: int, data: List[dict]):
        chunk_file = self.wip_dir / f"chunk_{chunk_index}.json"
        with self._lock:
            with chunk_file.open('w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

    def merge_chunks(self, output_file: Path):
        merged_data = []
        for chunk_file in sorted(self.wip_dir.glob("chunk_*.json")):
            with chunk_file.open('r', encoding='utf-8') as f:
                merged_data.extend(json.load(f))
            chunk_file.unlink()

        with output_file.open('w', encoding='utf-8') as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=4)


# === Основной класс конвертера ===
class DataConverter:
    def __init__(
        self,
        input_file: Path,
        output_file: Path,
        reader_cls: type = ExcelReader,
        chunk_size: int = 500,
        max_workers: Optional[int] = None,
        **reader_kwargs
    ):
        self.input_file = input_file
        self.reader_cls = reader_cls
        self.reader_kwargs = reader_kwargs
        self.chunk_size = chunk_size
        self.max_workers = max_workers or (os.cpu_count() or 1) - 1
        self.checkpoint = SQLiteCheckpoint(WIP_DIR / "checkpoint.db")
        self.monitor = SystemMonitor()
        self.logger = MultiProcessLogger(Path("logs")).get_logger("converter")
        self.chunk_writer = ChunkWriter(WIP_DIR)
        self.reader = self.reader_cls(input_file, **reader_kwargs)

    def convert(self, test_mode: bool = False):
        self.reader.validate()
        metadata = self.reader.get_metadata()

        try:
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                for sheet_name, sheet_meta in metadata.items():
                    total_rows = sheet_meta["rows"]
                    total_chunks = (total_rows + self.chunk_size - 1) // self.chunk_size
                    self.logger.info(f"Лист: {sheet_name}, строк: {total_rows}, предсказано чанков: {total_chunks}")

                    processed = self.checkpoint.load(sheet_name)

                    with tqdm(total=total_rows, desc=f"Processing sheet: {sheet_name}", unit="row") as pbar:
                        futures = {}
                        for start in range(0, total_rows, self.chunk_size):
                            chunk_index = start // self.chunk_size
                            if chunk_index in processed:
                                pbar.update(min(self.chunk_size, total_rows - start))
                                continue

                            if test_mode and chunk_index >= 3:  # Ограничение для тестового прогона
                                break

                            future = executor.submit(
                                process_chunk_function,
                                self.reader_cls,
                                self.input_file,
                                sheet_name,
                                start,
                                min(self.chunk_size, total_rows - start),
                                self.reader_kwargs
                            )
                            futures[future] = (sheet_name, chunk_index, start)

                        for future in as_completed(futures):
                            sheet_name, chunk_index, start = futures[future]
                            try:
                                results = future.result()
                                self.chunk_writer.write_chunk(chunk_index, results)
                                self.checkpoint.save(sheet_name, chunk_index, start + len(results))
                                self.logger.info(f"Обработан чанк {chunk_index}: строк {len(results)}")

                                resources = self.monitor.check_resources()
                                if resources["is_critical"]:
                                    self.logger.warning(
                                        f"High resource usage detected: CPU={resources['cpu_usage']}%, "
                                        f"Memory={resources['memory_usage']}%"
                                    )
                            except Exception as e:
                                self.logger.error(f"Error processing chunk {chunk_index}: {e}")
                            finally:
                                pbar.update(min(self.chunk_size, total_rows - start))
        except KeyboardInterrupt:
            self.logger.info("Graceful shutdown initiated...")
            sys.exit(0)
        finally:
            self.logger.info("Объединение чанков в итоговый файл...")
            output_json = OUTPUT_DIR / ("test_output.json" if test_mode else "output.json")
            self.chunk_writer.merge_chunks(output_json)

            # Проверка количества строк
            total_rows_in_json = self.verify_output(output_json, metadata)
            self.logger.info(f"Количество строк в JSON: {total_rows_in_json}")

            # Копирование базы данных в Output
            checkpoint_db = WIP_DIR / "checkpoint.db"
            output_db = OUTPUT_DIR / "output.db"
            shutil.copy(checkpoint_db, output_db)
            self.logger.info("База данных скопирована в Output.")

            # Закрытие соединения с базой данных
            self.checkpoint.close()

    def verify_output(self, output_file: Path, metadata: Dict[str, Any]) -> int:
        """Проверяет соответствие количества строк в JSON и исходном файле."""
        total_rows_in_json = 0
        with output_file.open('r', encoding='utf-8') as f:
            data = json.load(f)
            total_rows_in_json = len(data)

        total_rows_in_excel = sum(sheet_meta["rows"] for sheet_meta in metadata.values())
        if total_rows_in_json != total_rows_in_excel:
            self.logger.warning(
                f"Несоответствие данных! Строк в JSON: {total_rows_in_json}, строк в Excel: {total_rows_in_excel}"
            )
        else:
            self.logger.info("Данные успешно проверены. Все строки совпадают.")

        return total_rows_in_json


# === Entry Point ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Universal Data Converter")
    parser.add_argument("--format", choices=["excel", "csv"], default="excel", help="Input file format")
    parser.add_argument("--chunk-size", type=int, default=500, help="Chunk size for processing")
    parser.add_argument("--workers", type=int, help="Number of worker processes")
    parser.add_argument("--csv-separator", default=",", help="CSV separator (for CSV files)")
    args = parser.parse_args()

    # Очистка папки WIP
    clear_wip_directory()

    # Поиск входного файла
    input_files = list(INPUT_DIR.glob("*.xlsx")) + list(INPUT_DIR.glob("*.xls")) + list(INPUT_DIR.glob("*.csv"))
    if not input_files:
        raise FileNotFoundError("No input files found in the 'Input' directory.")
    input_file = input_files[0]

    # Определение формата файла
    if input_file.suffix.lower() in [".xlsx", ".xls"]:
        reader_cls = ExcelReader
        reader_kwargs = {}
    elif input_file.suffix.lower() == ".csv":
        reader_cls = CSVReader
        reader_kwargs = {"sep": args.csv_separator}
    else:
        raise ValueError("Unsupported file format.")

    # Тестовый прогон
    print("Запуск тестового прогона на 2-3 чанках...")
    converter = DataConverter(
        input_file=input_file,
        output_file=OUTPUT_DIR / "test_output.json",
        reader_cls=reader_cls,
        chunk_size=args.chunk_size,
        max_workers=args.workers,
        **reader_kwargs
    )
    converter.convert(test_mode=True)

    print("\nРезультаты тестового прогона:")
    print("- Чанки обработаны и сохранены в Output/test_output.json.")
    print("- База данных сохранена в Output/output.db.")
    print("- Логи доступны в папке logs/.")

    # Запрос подтверждения для полноценного запуска
    full_run = input("Хотите запустить полноценную обработку? (Y/N): ").strip().lower()
    if full_run == "y":
        print("Запуск полноценной обработки...")
        converter.checkpoint.close()  # Закрытие соединения с базой данных
        clear_checkpoints(WIP_DIR / "checkpoint.db")  # Очистка чекпоинтов
        converter = DataConverter(
            input_file=input_file,
            output_file=OUTPUT_DIR / "output.json",
            reader_cls=reader_cls,
            chunk_size=args.chunk_size,
            max_workers=args.workers,
            **reader_kwargs
        )
        converter.convert(test_mode=False)
        print("Обработка завершена. Результаты сохранены в Output/output.json и Output/output.db.")
    else:
        print("Программа завершена без полноценной обработки.")