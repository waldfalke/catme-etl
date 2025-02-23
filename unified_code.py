import os
import sys
import json
import logging
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
INPUT_DIR: Path = Path("Input")
WIP_DIR: Path = Path("WIP")
OUTPUT_DIR: Path = Path("Output")

for dir_path in [INPUT_DIR, WIP_DIR, OUTPUT_DIR]:
    dir_path.mkdir(exist_ok=True)

# === Функции очистки директорий и чекпоинтов ===
def clear_wip_directory() -> None:
    """
    Удаляет все файлы и папки в рабочей директории WIP.
    """
    for file in WIP_DIR.iterdir():
        if file.is_file():
            file.unlink()
        elif file.is_dir():
            shutil.rmtree(file)


def clear_checkpoints(checkpoint_db: Path) -> None:
    """
    Удаляет файл базы данных чекпоинтов.
    """
    if checkpoint_db.exists():
        try:
            checkpoint_db.unlink()
        except PermissionError as e:
            print(f"Ошибка при удалении файла {checkpoint_db}: {e}")

# === Конфигурация логгера ===
class MultiProcessLogger:
    def __init__(self, log_dir: Path) -> None:
        self.log_dir = log_dir
        self.log_dir.mkdir(exist_ok=True)
        self._lock = threading.Lock()

    def get_logger(self, name: str) -> logging.Logger:
        """
        Создает или возвращает логгер для указанного имени.
        """
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
    def __init__(self, threshold_cpu: float = 90.0, threshold_memory: float = 90.0) -> None:
        self.threshold_cpu = threshold_cpu
        self.threshold_memory = threshold_memory
        self._start_time = datetime.now()

    def check_resources(self) -> Dict[str, Any]:
        """
        Проверяет использование ресурсов системы.
        """
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
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._conn = sqlite3.connect(str(self.db_path))
        self._init_db()

    def _init_db(self) -> None:
        """
        Инициализирует структуру базы данных.
        """
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

    def save(self, sheet_name: str, chunk_index: int, processed_rows: int) -> None:
        """
        Сохраняет состояние обработки чанка в базе данных.
        """
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO checkpoints (sheet_name, chunk_index, processed_rows) VALUES (?, ?, ?)",
                (sheet_name, chunk_index, processed_rows)
            )

    def load(self, sheet_name: str) -> Dict[int, int]:
        """
        Загружает состояние обработки чанков для указанного листа.
        """
        cursor = self._conn.execute(
            "SELECT chunk_index, processed_rows FROM checkpoints WHERE sheet_name = ?",
            (sheet_name,)
        )
        return dict(cursor.fetchall())

    def close(self) -> None:
        """
        Закрывает соединение с базой данных.
        """
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
    def __init__(self, file_path: Path, **kwargs: Any) -> None:
        self.file_path = file_path

    def validate(self) -> bool:
        """
        Проверяет корректность Excel-файла.
        """
        try:
            pd.read_excel(self.file_path, nrows=0)
            return True
        except Exception as e:
            raise ValueError(f"Invalid Excel file: {e}")

    def get_metadata(self) -> Dict[str, Any]:
        """
        Получает метаданные из Excel-файла.
        """
        excel = pd.ExcelFile(self.file_path)
        metadata: Dict[str, Any] = {}
        for sheet in excel.sheet_names:
            df = excel.parse(sheet, nrows=0)
            metadata[sheet] = {
                "rows": excel.parse(sheet).shape[0],
                "columns": df.columns.tolist(),
                "dtypes": df.dtypes.to_dict()
            }
        return metadata

    def read_chunk(self, sheet_name: str, start: int, size: int) -> pd.DataFrame:
        """
        Читает часть данных из Excel-файла.
        """
        return pd.read_excel(
            self.file_path,
            sheet_name=sheet_name,
            skiprows=range(1, start + 1) if start > 0 else None,
            nrows=size
        )

class CSVReader(DataReader):
    def __init__(self, file_path: Path, **kwargs: Any) -> None:
        self.file_path = file_path
        self.csv_kwargs = kwargs

    def validate(self) -> bool:
        """
        Проверяет корректность CSV-файла.
        """
        try:
            pd.read_csv(self.file_path, nrows=0, **self.csv_kwargs)
            return True
        except Exception as e:
            raise ValueError(f"Invalid CSV file: {e}")

    def get_metadata(self) -> Dict[str, Any]:
        """
        Получает метаданные из CSV-файла.
        """
        df = pd.read_csv(self.file_path, nrows=0, **self.csv_kwargs)
        return {
            "default": {
                "rows": sum(1 for _ in open(self.file_path)) - 1,
                "columns": df.columns.tolist(),
                "dtypes": df.dtypes.to_dict()
            }
        }

    def read_chunk(self, sheet_name: str, start: int, size: int) -> pd.DataFrame:
        """
        Читает часть данных из CSV-файла.
        Параметр sheet_name игнорируется, но сохраняется для единообразия интерфейса.
        """
        return pd.read_csv(
            self.file_path,
            skiprows=range(1, start + 1) if start > 0 else None,
            nrows=size,
            **self.csv_kwargs
        )

# === Этапы обработки чанка с явными контрактами и плавным ростом сложности ===

def prepare_chunk(chunk: List[dict]) -> List[dict]:
    """
    Подготавливает чанк данных.

    Предусловия: входной чанк является списком непустых словарей.
    Постусловия: каждому элементу добавляется ключ 'prepared': True.
    """
    for record in chunk:
        record['prepared'] = True
    return chunk

def validate_chunk(chunk: List[dict]) -> List[dict]:
    """
    Валидирует подготовленный чанк данных.

    Предусловия: каждый элемент должен содержать ключ 'prepared'.
    Постусловия: при отсутствии ключа 'id' он добавляется со значением None.
    """
    for record in chunk:
        if 'prepared' not in record:
            raise ValueError("Запись не подготовлена!")
        if 'id' not in record:
            record['id'] = None
    return chunk

def transform_chunk(chunk: List[dict]) -> List[dict]:
    """
    Трансформирует валидированный чанк данных.

    Предусловия: чанк валидирован (каждая запись содержит ключ 'prepared').
    Постусловия: каждому элементу добавляется ключ 'transformed': True.
    """
    for record in chunk:
        record['transformed'] = True
    return chunk

def finalize_chunk(chunk: List[dict]) -> List[dict]:
    """
    Финализирует обработанный чанк данных.

    Предусловия: все записи содержат ключи 'prepared' и 'transformed'.
    Постусловия: каждому элементу добавляется ключ 'finalized': True.
    """
    for record in chunk:
        if not (record.get('prepared') and record.get('transformed')):
            raise ValueError("Запись не готова к финализации!")
        record['finalized'] = True
    return chunk

# === Топ-уровневая функция обработки чанка с якорями и этапами ===
def process_chunk_function(reader_cls: type, file_path: Path, sheet_name: str, start: int, size: int, reader_kwargs: dict) -> List[dict]:
    """
    Обрабатывает указанный чанк данных с постепенным ростом сложности.

    Этапы обработки:
      1. Подготовка данных (prepare)
      2. Валидация данных (validate)
      3. Трансформация данных (transform)
      4. Финализация данных (finalize)

    Важно: 
      ### BEGIN CRITICAL SECTION ###
      - Этапы разделены якорными комментариями для явного документирования ключевых точек.
      ### END CRITICAL SECTION ###
    """
    # Создание ридера и чтение данных
    reader: DataReader = reader_cls(file_path, **reader_kwargs)
    df = reader.read_chunk(sheet_name, start, size)
    raw_chunk = df.to_dict(orient="records")
    
    # BEGIN CRITICAL SECTION: начало обработки чанка
    ### BEGIN PREPARATION ###
    prepared_chunk = prepare_chunk(raw_chunk)
    ### END PREPARATION ###
    
    ### BEGIN VALIDATION ###
    validated_chunk = validate_chunk(prepared_chunk)
    ### END VALIDATION ###
    
    ### BEGIN TRANSFORMATION ###
    transformed_chunk = transform_chunk(validated_chunk)
    ### END TRANSFORMATION ###
    
    ### BEGIN FINALIZATION ###
    final_chunk = finalize_chunk(transformed_chunk)
    ### END FINALIZATION ###
    # END CRITICAL SECTION
    
    return final_chunk

# === Класс для записи временных JSON-файлов ===
class ChunkWriter:
    def __init__(self, wip_dir: Path) -> None:
        self.wip_dir = wip_dir
        self.wip_dir.mkdir(exist_ok=True)
        self._lock = threading.Lock()

    def write_chunk(self, chunk_index: int, data: List[dict]) -> None:
        """
        Записывает данные чанка во временный JSON-файл.
        """
        chunk_file = self.wip_dir / f"chunk_{chunk_index}.json"
        with self._lock:
            with chunk_file.open('w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

    def merge_chunks(self, output_file: Path) -> None:
        """
        Объединяет все временные JSON-файлы в один итоговый файл.
        """
        merged_data: List[Any] = []
        for chunk_file in sorted(self.wip_dir.glob("chunk_*.json")):
            with chunk_file.open('r', encoding='utf-8') as f:
                merged_data.extend(json.load(f))
            chunk_file.unlink()
        with output_file.open('w', encoding='utf-8') as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=4)

# === Функция оптимизации JSON ===
def optimize_json(input_path: Path, output_path: Path) -> Dict[str, float]:
    """
    Оптимизирует JSON, убирая избыточное форматирование, и сохраняет оптимизированную версию.
    
    Возвращает статистику оптимизации:
      - original_size_mb: исходный размер в МБ
      - optimized_size_mb: размер после оптимизации в МБ
      - saved_space_mb: сэкономленный объём в МБ
      - reduction_percent: процент уменьшения размера
    """
    original_size = input_path.stat().st_size
    with input_path.open('r', encoding='utf-8') as f:
        data = json.load(f)
    with output_path.open('w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
    optimized_size = output_path.stat().st_size
    saved_space = original_size - optimized_size
    reduction_percent = (saved_space / original_size * 100) if original_size else 0
    return {
        'original_size_mb': original_size / (1024 * 1024),
        'optimized_size_mb': optimized_size / (1024 * 1024),
        'saved_space_mb': saved_space / (1024 * 1024),
        'reduction_percent': reduction_percent
    }

# === Основной класс конвертера ===
class DataConverter:
    def __init__(
        self,
        input_file: Path,
        output_file: Path,
        reader_cls: type = ExcelReader,
        chunk_size: int = 500,
        max_workers: Optional[int] = None,
        **reader_kwargs: Any
    ) -> None:
        self.input_file = input_file
        self.output_file = output_file
        self.reader_cls = reader_cls
        self.reader_kwargs = reader_kwargs
        self.chunk_size = chunk_size
        self.max_workers = max_workers or (os.cpu_count() or 1) - 1
        self.checkpoint = SQLiteCheckpoint(WIP_DIR / "checkpoint.db")
        self.monitor = SystemMonitor()
        self.logger = MultiProcessLogger(Path("logs")).get_logger("converter")
        self.chunk_writer = ChunkWriter(WIP_DIR)
        self.reader: DataReader = self.reader_cls(input_file, **reader_kwargs)

    def convert(self, test_mode: bool = False) -> None:
        """
        Основной метод для конвертации данных.
        
        Если test_mode=True, обрабатываются только 2–3 чанка для контроля,
        затем создается оптимизированный JSON для проверки и выводится статистика.
        В обоих случаях результат сохраняется в итоговом JSON-файле.
        """
        self.reader.validate()
        metadata = self.reader.get_metadata()

        try:
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                for sheet_name, sheet_meta in metadata.items():
                    total_rows: int = sheet_meta["rows"]
                    total_chunks: int = (total_rows + self.chunk_size - 1) // self.chunk_size
                    self.logger.info(f"Лист: {sheet_name}, строк: {total_rows}, предсказано чанков: {total_chunks}")

                    processed = self.checkpoint.load(sheet_name)

                    with tqdm(total=total_rows, desc=f"Processing sheet: {sheet_name}", unit="row") as pbar:
                        futures: Dict[Any, Any] = {}
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
            # Объединение всех обработанных чанков в итоговый JSON
            self.chunk_writer.merge_chunks(self.output_file)

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
    input_files: List[Path] = list(INPUT_DIR.glob("*.xlsx")) + list(INPUT_DIR.glob("*.xls")) + list(INPUT_DIR.glob("*.csv"))
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

    # Запрос подтверждения на тестовый прогон
    test_run_choice = input("Запустить тестовый прогон на 2–3 чанка для контроля? (Y/N): ").strip().lower()
    if test_run_choice != "y":
        print("Тестовый прогон отменён пользователем. Завершение программы.")
        sys.exit(0)

    # Тестовый прогон с записью оптимизированного JSON и подробной статистикой
    print("Запуск тестового прогона (обрабатываются 2–3 чанка)...")
    test_output_file = OUTPUT_DIR / "test_output.json"
    converter = DataConverter(
        input_file=input_file,
        output_file=test_output_file,
        reader_cls=reader_cls,
        chunk_size=args.chunk_size,
        max_workers=args.workers,
        **reader_kwargs
    )
    converter.convert(test_mode=True)

    # После тестового прогона оптимизируем полученный JSON
    optimized_test_output = OUTPUT_DIR / "test_output_optimized.json"
    optimizer_logger = MultiProcessLogger(Path("logs")).get_logger("optimizer")
    try:
        stats = optimize_json(test_output_file, optimized_test_output)
        optimizer_logger.info("Статистика оптимизации тестового прогона:")
        optimizer_logger.info(f"Исходный размер: {stats['original_size_mb']:.2f} MB")
        optimizer_logger.info(f"Размер после оптимизации: {stats['optimized_size_mb']:.2f} MB")
        optimizer_logger.info(f"Сэкономлено места: {stats['saved_space_mb']:.2f} MB")
        optimizer_logger.info(f"Уменьшение размера: {stats['reduction_percent']:.1f}%")
        print(f"\nТестовый прогон завершён.\nОптимизированный JSON сохранён в: {optimized_test_output}")
        print("Статистика оптимизации (см. логи 'optimizer'):")
        print(f"  Исходный размер: {stats['original_size_mb']:.2f} MB")
        print(f"  Размер после оптимизации: {stats['optimized_size_mb']:.2f} MB")
        print(f"  Сэкономлено: {stats['saved_space_mb']:.2f} MB ({stats['reduction_percent']:.1f}% уменьшение)")
    except Exception as e:
        optimizer_logger.error(f"Ошибка при оптимизации JSON тестового прогона: {e}")
        print(f"Ошибка при оптимизации тестового JSON: {e}")

    # Если тестовый прогон выполнен успешно, предлагаем полноценный запуск
    full_run = input("\nХотите запустить полноценную обработку? (Y/N): ").strip().lower()
    if full_run == "y":
        print("Запуск полноценной обработки...")
        converter.checkpoint.close()  # Закрытие соединения с базой данных
        clear_checkpoints(WIP_DIR / "checkpoint.db")  # Очистка чекпоинтов

        full_output_file = OUTPUT_DIR / "output.json"
        converter = DataConverter(
            input_file=input_file,
            output_file=full_output_file,
            reader_cls=reader_cls,
            chunk_size=args.chunk_size,
            max_workers=args.workers,
            **reader_kwargs
        )
        converter.convert(test_mode=False)

        print("Обработка завершена. Результаты сохранены в:")
        print(f"  Итоговый JSON: {full_output_file}")
        print(f"  Чекпоинты: {WIP_DIR / 'checkpoint.db'}")

        # Оптимизация итогового JSON
        optimized_full_output = OUTPUT_DIR / "output_optimized.json"
        full_optimizer_logger = MultiProcessLogger(Path("logs")).get_logger("optimizer")
        try:
            stats = optimize_json(full_output_file, optimized_full_output)
            full_optimizer_logger.info("Статистика оптимизации итогового JSON:")
            full_optimizer_logger.info(f"Исходный размер: {stats['original_size_mb']:.2f} MB")
            full_optimizer_logger.info(f"Размер после оптимизации: {stats['optimized_size_mb']:.2f} MB")
            full_optimizer_logger.info(f"Сэкономлено места: {stats['saved_space_mb']:.2f} MB")
            full_optimizer_logger.info(f"Уменьшение размера: {stats['reduction_percent']:.1f}%")
            print(f"\nОптимизированный итоговый JSON сохранён в: {optimized_full_output}")
        except Exception as e:
            full_optimizer_logger.error(f"Ошибка при оптимизации итогового JSON: {e}")
            print(f"Ошибка при оптимизации итогового JSON: {e}")
    else:
        print("Полноценная обработка отменена. Программа завершена.")
