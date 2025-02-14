import os
import sys
import hashlib
from collections import defaultdict
import argparse
import logging
from typing import Dict, List, Optional
import concurrent.futures


class FileDuplicateFinder:
    """
    Класс для поиска дубликатов файлов в указанной директории.
    Поддерживает игнорирование файлов по паттернам и минимальный размер файла для проверки.
    """

    def __init__(self, directory: str, 
                 ignore_patterns: Optional[List[str]] = None, 
                 min_file_size: int = 1):
        """
        Инициализация объекта для поиска дубликатов.

        :param directory: Директория для сканирования.
        :param ignore_patterns: Список паттернов для игнорирования файлов.
        :param min_file_size: Минимальный размер файла для проверки (в байтах).
        """
        self.directory = os.path.abspath(directory)
        self.ignore_patterns = ignore_patterns or []
        self.min_file_size = min_file_size
        
        # Настройка логирования
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _should_ignore_file(self, filename: str) -> bool:
        """
        Проверяет, нужно ли игнорировать файл по заданным паттернам.

        :param filename: Имя файла.
        :return: True, если файл нужно игнорировать, иначе False.
        """
        return any(pattern in filename for pattern in self.ignore_patterns)

    def calculate_hash(self, file_path: str, chunk_size: int = 8192) -> Optional[str]:
        """
        Вычисляет хеш файла с использованием алгоритма SHA-256.

        :param file_path: Путь к файлу.
        :param chunk_size: Размер блока для чтения файла.
        :return: Хеш файла или None, если произошла ошибка.
        """
        try:
            hasher = hashlib.sha256()
            with open(file_path, 'rb') as f:
                while chunk := f.read(chunk_size):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except PermissionError:
            self.logger.warning(f"Нет доступа к файлу: {file_path}")
        except OSError as e:
            self.logger.error(f"Ошибка при чтении файла {file_path}: {e}")
        return None

    def find_duplicate_files(self) -> Dict[str, List[str]]:
        """
        Ищет дубликаты файлов в указанной директории.

        :return: Словарь, где ключ — хеш файла, а значение — список путей к файлам с этим хешом.
        """
        file_hashes = defaultdict(list)
        files_to_check = []

        # Рекурсивно обходим директорию
        for root, _, files in os.walk(self.directory):
            for filename in files:
                file_path = os.path.join(root, filename)
                
                # Проверяем, соответствует ли файл критериям
                if (os.path.getsize(file_path) >= self.min_file_size and 
                    not self._should_ignore_file(filename)):
                    files_to_check.append(file_path)

        # Используем ThreadPoolExecutor для параллельного вычисления хешей
        with concurrent.futures.ThreadPoolExecutor() as executor:
            hash_results = list(executor.map(self.calculate_hash, files_to_check))

        # Группируем файлы по их хешам
        for file_path, file_hash in zip(files_to_check, hash_results):
            if file_hash:
                file_hashes[file_hash].append(file_path)

        # Возвращаем только те хеши, у которых больше одного файла
        return {hash_val: paths for hash_val, paths in file_hashes.items() if len(paths) > 1}

    def print_duplicates(self, duplicates: Dict[str, List[str]]):
        """
        Выводит информацию о найденных дубликатах.

        :param duplicates: Словарь с дубликатами файлов.
        """
        if not duplicates:
            self.logger.info("Дубликаты не найдены.")
            return

        self.logger.info(f"Найдено {len(duplicates)} групп дубликатов:")
        for i, (hash_val, file_paths) in enumerate(duplicates.items(), 1):
            total_size = sum(os.path.getsize(path) for path in file_paths)
            print(f"\n📁 Группа {i}:")
            print(f"   Хеш: {hash_val}")
            print(f"   Размер группы: {len(file_paths)} файлов")
            print(f"   Общий размер: {total_size / (1024 * 1024):.2f} МБ")
            for path in file_paths:
                print(f"   - {path}")


def main():
    """
    Основная функция для запуска поиска дубликатов.
    """
    parser = argparse.ArgumentParser(description="Поиск дубликатов файлов.")
    parser.add_argument("directory", help="Директория для сканирования")
    parser.add_argument("--ignore", nargs="+", 
                        help="Паттерны файлов для игнорирования")
    parser.add_argument("--min-size", type=int, default=1024, 
                        help="Минимальный размер файла для проверки (байты)")
    
    args = parser.parse_args()

    try:
        # Создаем объект для поиска дубликатов
        finder = FileDuplicateFinder(
            directory=args.directory, 
            ignore_patterns=args.ignore or [],
            min_file_size=args.min_size
        )
        # Ищем дубликаты
        duplicates = finder.find_duplicate_files()
        # Выводим результаты
        finder.print_duplicates(duplicates)
    
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()