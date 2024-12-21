import pandas as pd
import argparse
import os
from algorithm import Algorithm


def fetch_data_paths(data_dir):
    """
    Возвращает пути к файлам данных в правильном порядке для инициализации класса.
    """
    paths = {'payments': None, 'providers': None, 'ex_rates': None}
    
    for filename in os.listdir(data_dir):
        if "payments" in filename:
            paths['payments'] = os.path.join(data_dir, filename)
        elif "providers" in filename:
            paths['providers'] = os.path.join(data_dir, filename)
        elif "ex_rates" in filename:
            paths['ex_rates'] = os.path.join(data_dir, filename)
    
    if None in paths.values():
        raise FileNotFoundError("Отсутствуют один или несколько необходимых файлов (payments, providers, ex_rates).")
    
    return paths


def main(args):
    # Пути к файлам
    folder_path = args.data_dir
    data_paths = fetch_data_paths(folder_path)
    
    
    algorithm = Algorithm(
        payments_path=data_paths['payments'],
        providers_path=data_paths['providers'],
        ex_rates_path=data_paths['ex_rates']
    )
    
    # TODO: algos
    
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Скрипт для обработки файлов с данными.")
    parser.add_argument("--data_dir", type=str, required=True, help="Путь до папки с данными")
    args = parser.parse_args()
    main(args)
