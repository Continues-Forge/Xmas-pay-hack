import pandas as pd
from tqdm import tqdm
import time


class Algorithm:
    def init(self, payments_file, providers_file, ex_rates_file):
        self.df_payments = pd.read_csv(payments_file)
        self.df_providers = pd.read_csv(providers_file).rename(columns={'CURRENCY': 'cur'})
        self.df_ex_rates = pd.read_csv(ex_rates_file)

    def preprocess(self):
        # Сортировка по eventTimeRes, получение временных меток
        self.df_payments['eventTimeRes'] = pd.to_datetime(self.df_payments['eventTimeRes'])
        self.df_payments = self.df_payments.sort_values(['eventTimeRes'])
        self.unique_time = self.df_payments['eventTimeRes'].unique()
        # Сортировка по TIME, получение первого вхождения лимитов
        self.df_providers['TIME'] = pd.to_datetime(self.df_providers['TIME'])
        self.df_providers = self.df_providers.sort_values(['TIME'])
        self.real_limits = self.df_providers[self.df_providers['TIME'] == self.df_providers['TIME'].iloc[0]][
            ['ID', 'LIMIT_MIN', 'LIMIT_MAX']]

    def dataloader(self):
        # Адаптация алгоритма под реальные условия
        self.speed = []
        with tqdm(self.unique_time, desc="Processing", dynamic_ncols=True) as pbar:
            for time in self.unique_time:
                payments_t = self.df_payments[self.df_payments['eventTimeRes'] == time]
                provider_t = pd.DataFrame()
                for unique_id in self.df_providers["ID"].unique():
                    pbt = self.df_providers[
                        (self.df_providers["ID"] == unique_id) & (self.df_providers["TIME"] <= time)]
                    max_time_row = pbt[pbt["TIME"] == pbt["TIME"].max()]
                    provider_t = pd.concat([provider_t, max_time_row], ignore_index=True)
                avg_speed = sum(self.speed) / len(self.speed) if len(self.speed) > 0 else 0.0
                pbar.set_postfix({"AVG speed": f"{avg_speed:.2f} p/s"})
                pbar.update(1)
                yield [provider_t, payments_t]

    def preprocess_batch(self, provider_t, payments_t):
        # Берем последнее вхождение информации о провайдере за промежуток времени, сортируем по времени + ID провайдера
        provider_t = provider_t.drop_duplicates(subset=['TIME', 'ID'], keep='last')
        provider_t = provider_t.sort_values(['TIME', 'ID'])
        filter_min_max = provider_t.groupby('cur').agg(absolute_min=("MIN_SUM", "min"),
                                                       absolute_max=("MAX_SUM", "max")).reset_index()
        # Выделяем провайдеров по cur
        self.providers_by_cur = {cur: group.sort_values(by='TIME', ascending=False) for cur, group in
                                 provider_t.groupby('cur')}
        # Перевод валюты в USD + приоритезация переводов большей суммы
        ex_rates_dict = dict(self.df_ex_rates.values)
        ex_rates_dict = dict((v, k) for k, v in ex_rates_dict.items())
        payments_t = payments_t.copy()
        payments_t['amount_in_USD'] = payments_t['cur'].map(lambda x: ex_rates_dict[x]) * payments_t['amount']
        payments_t = payments_t.sort_values(['amount_in_USD'])[::-1]
        # Оставляем только провайдеров входящих в диапазон абсолютных значений по min max для данног cur
        payments_t = pd.merge(payments_t, filter_min_max, on="cur", how="inner")
        payments_t["flow"] = None
        payments_t.loc[(payments_t["amount"] >= payments_t["absolute_min"]) & (
                    payments_t["amount"] <= payments_t["absolute_max"]), "flow"] = 1
        payments_t = payments_t.drop(columns=["absolute_min", "absolute_max"])
        return (provider_t, payments_t)

    def find_closest_provider(self, row):
        # Пропускаем, если не прошло по фильтрации во время предобработки батча
        if row['flow'] == None: return None
        # Получаем соответствующую группу провайдеров по валюте
        temp_providers = self.providers_by_cur.get(row['cur'], pd.DataFrame())
        # Если нет подходящих провайдеров, возвращаем пустой список
        if temp_providers.empty: return None
        # Сортируем по комиссии и среднему времени
        temp_providers = temp_providers.sort_values(by=['COMMISSION', 'AVG_TIME', 'CONVERSION'],
                                                    ascending=[True, True, False])
        return '-'.join(list(map(lambda i: str(i), temp_providers['ID'])))


    def run(self):
        for batch in self.dataloader():
            # Старт таймера
            start_time = time.time()
            provider_t, payments_t = batch
            provider_t, payments_t = self.preprocess_batch(provider_t, payments_t)
            payments_t['flow'] = payments_t.apply(self.find_closest_provider, axis=1)
            # Конец таймера
            end_time = time.time()

            batch_speed = len(payments_t) / (end_time - start_time)
            self.speed.append(batch_speed)