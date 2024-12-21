import argparse
import os
from algorithm import Algorithm
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
    
    alg = Algorithm(
        payments_file=data_paths['payments'],
        providers_file=data_paths['providers'],
        ex_rates_file=data_paths['ex_rates']
    )
    logging.log(level=20, msg='Инициализирован класс')

    # Приводим данные в нормальный вид
    alg.preprocess()
    logging.log(level=20, msg='Данные причесаны')

    alg.run()
    logging.log(level=20, msg='Решение завершено !!!')

    alg.df_payments.to_csv('res.csv', index=False)
    logging.log(level=20, msg='Файл сохранен !!!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Скрипт для обработки файлов с данными.")
    parser.add_argument("--data_dir", type=str, required=True, help="Путь до папки с данными")
    args = parser.parse_args()
    main(args)
    
    while True:
        pass
