import os
import random
import heapq
import time

CHUNK_SIZE_MB = 100  # Розмір однієї серії в мегабайтах


def generate_large_file_without_memory_limit(filename, size_in_mb):
    """
    Генерує великий файл із випадковими числами.

    :param filename: Назва файлу для збереження.
    :param size_in_mb: Розмір файлу в мегабайтах.
    """
    num_elements = (size_in_mb * 1024 * 1024) // 7  # 1 число ≈ 7 байт
    with open(filename, "w") as f:
        for _ in range(num_elements):
            f.write(f"{random.randint(0, 10 ** 6)}\n")


def split_into_sorted_chunks(input_file, chunk_size_mb):
    """
    Розбиває великий файл на відсортовані серії.

    :param input_file: Шлях до великого файлу.
    :param chunk_size_mb: Розмір однієї серії у мегабайтах.
    :return: Список імен тимчасових файлів із відсортованими серіями.
    """
    chunk_files = []
    chunk_size = (chunk_size_mb * 1024 * 1024) // 7  # Кількість чисел у серії
    with open(input_file, "r") as f:
        buffer = []
        chunk_index = 0
        for line in f:
            buffer.append(int(line.strip()))
            if len(buffer) >= chunk_size:
                chunk_index += 1
                chunk_files.append(save_sorted_chunk(buffer, chunk_index))
                buffer = []
        if buffer:
            chunk_index += 1
            chunk_files.append(save_sorted_chunk(buffer, chunk_index))
    return chunk_files


def save_sorted_chunk(data, chunk_index):
    """
    Сортує серію та зберігає її у тимчасовий файл.

    :param data: Список чисел для сортування.
    :param chunk_index: Номер серії.
    :return: Ім'я тимчасового файлу.
    """
    data.sort()
    chunk_filename = f"chunk_{chunk_index}.txt"
    with open(chunk_filename, "w") as f:
        f.write("\n".join(map(str, data)))
    return chunk_filename


def merge_sorted_chunks(chunk_files, output_file):
    """
    Зливає відсортовані серії у один вихідний файл.

    :param chunk_files: Список імен файлів із серіями.
    :param output_file: Ім'я вихідного файлу.
    """
    min_heap = []
    file_pointers = [open(chunk, "r") for chunk in chunk_files]

    for i, fp in enumerate(file_pointers):
        line = fp.readline().strip()
        if line:
            heapq.heappush(min_heap, (int(line), i))

    with open(output_file, "w") as out:
        while min_heap:
            min_value, file_index = heapq.heappop(min_heap)
            out.write(f"{min_value}\n")
            next_line = file_pointers[file_index].readline().strip()
            if next_line:
                heapq.heappush(min_heap, (int(next_line), file_index))

    for fp in file_pointers:
        fp.close()
    for chunk in chunk_files:
        os.remove(chunk)


if __name__ == "__main__":
    INPUT_FILE = "large_input_no_limit.txt"
    OUTPUT_FILE = "sorted_output.txt"
    FILE_SIZE_MB = 1024  # Розмір файлу в мб

    # Генерація великого файлу
    print("Генерація великого файлу...")
    start_time = time.time()
    generate_large_file_without_memory_limit(INPUT_FILE, FILE_SIZE_MB)
    generate_time = time.time() - start_time
    print(f"Файл '{INPUT_FILE}' створено. Час генерації: {generate_time:.2f} секунд.\n")

    # Розбиття на серії
    print("Розбиття на серії...")
    start_time = time.time()
    chunk_files = split_into_sorted_chunks(INPUT_FILE, CHUNK_SIZE_MB)
    split_time = time.time() - start_time
    print(f"Розбито на {len(chunk_files)} серій. Час розбиття: {split_time:.2f} секунд.\n")

    # Злиття серій
    print("Злиття серій...")
    start_time = time.time()
    merge_sorted_chunks(chunk_files, OUTPUT_FILE)
    merge_time = time.time() - start_time
    print(f"Сортування завершено. Вихідний файл: '{OUTPUT_FILE}'. Час злиття: {merge_time:.2f} секунд.\n")

    # Загальний час
    total_time = generate_time + split_time + merge_time
    print(f"Загальний час виконання: {total_time:.2f} секунд.")
