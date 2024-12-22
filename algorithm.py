import pandas as pd


class Algorithm:
    def __init__(self, payments_file, providers_file, ex_rates_file):
        self.df_payments = pd.read_csv(payments_file)
        self.df_providers = pd.read_csv(providers_file)
        self.df_ex_rates = pd.read_csv(ex_rates_file)

    def preprocess(self):
        # дропаем ненужный столбец
        self.df_providers.drop(['LIMIT_BY_CARD'], axis=1, inplace=True)

        # переводим все к USD
        df_ex_rates_values = dict(self.df_ex_rates.values)
        dict_ex_rates_values = dict((v, k) for k, v in df_ex_rates_values.items())

        self.df_payments['amount_in_USD'] = self.df_payments['cur'].map(lambda x: dict_ex_rates_values[x]) * \
                                            self.df_payments['amount']

        self.df_providers = self.df_providers.rename(columns={'CURRENCY': 'cur'})
        self.df_providers['MIN_SUM_in_USD'] = self.df_providers['cur'].map(lambda x: dict_ex_rates_values[x]) * \
                                              self.df_providers[
                                                  'MIN_SUM']
        self.df_providers['MAX_SUM_in_USD'] = self.df_providers['cur'].map(lambda x: dict_ex_rates_values[x]) * \
                                              self.df_providers[
                                                  'MAX_SUM']
        self.df_providers['LIMIT_MIN_in_USD'] = self.df_providers['cur'].map(lambda x: dict_ex_rates_values[x]) * \
                                                self.df_providers[
                                                    'LIMIT_MIN']
        self.df_providers['LIMIT_MAX_in_USD'] = self.df_providers['cur'].map(lambda x: dict_ex_rates_values[x]) * \
                                                self.df_providers[
                                                    'LIMIT_MAX']

        # приводим к одному значению лимитов в день
        self.df_providers = self.set_lim_min_max(self.df_providers)

        # приводим все к datetime
        self.df_payments['TIME'] = pd.to_datetime(self.df_payments['eventTimeRes'])
        self.df_providers['TIME'] = pd.to_datetime(self.df_providers['TIME'])

        self.df_payments.sort_values(by=['TIME', 'amount_in_USD'], ascending=[True, False])

    def set_lim_min_max(self, df_providers):
        df_providers['TIME'] = pd.to_datetime(df_providers['TIME'])

        df_providers['DATE'] = df_providers['TIME'].dt.date

        first_entries = df_providers.groupby(['ID', 'DATE']).first().reset_index()
        first_entries = first_entries[['ID', 'DATE', 'LIMIT_MIN_in_USD', 'LIMIT_MAX_in_USD']]

        first_entries = first_entries.rename(
            columns={'LIMIT_MIN_in_USD': 'FIRST_LIMIT_MIN', 'LIMIT_MAX_in_USD': 'FIRST_LIMIT_MAX'})

        df_providers = df_providers.merge(first_entries, on=['ID', 'DATE'], how='left')

        df_providers['LIMIT_MIN_in_USD'] = df_providers['FIRST_LIMIT_MIN']
        df_providers['LIMIT_MAX_in_USD'] = df_providers['FIRST_LIMIT_MAX']

        df_providers = df_providers.drop(columns=['FIRST_LIMIT_MIN', 'FIRST_LIMIT_MAX', 'DATE'])
        return df_providers

    def run(self):
        def find_closest_provider(row):
            # Фильтруем по времени и суммам
            temp_providers = self.df_providers[
                (self.df_providers['TIME'] <= row['TIME']) &
                (self.df_providers['MIN_SUM_in_USD'] <= row['amount_in_USD']) &
                (row['amount_in_USD'] <= self.df_providers['MAX_SUM_in_USD'])
                ]

            # Если нет подходящих провайдеров, возвращаем пустой список
            if temp_providers.empty:
                return None

            temp_providers = temp_providers.sort_values(by='TIME', ascending=False)

            closest_provider = temp_providers.drop_duplicates(subset='ID', keep='first')

            closest_provider = closest_provider[closest_provider['cur'] == row['cur']]

            # Сортируем по комиссии и среднему времени
            closest_provider = closest_provider.sort_values(by=['COMMISSION', 'AVG_TIME', 'CONVERSION'],
                                                        ascending=[True, True, False])

            return '-'.join(list(map(lambda i: str(i), closest_provider['ID'])))

        self.df_payments['flow'] = self.df_payments.apply(find_closest_provider, axis=1)

        self.df_payments.drop(columns=['TIME', 'amount_in_USD'], inplace=True)
