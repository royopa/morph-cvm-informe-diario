
# -*- coding: utf-8 -*-
import datetime
import os
import shutil

import pandas as pd
import scraperwiki
from sqlalchemy import create_engine


def main():
    today = datetime.date.today()
    ano_inicial = 2017
    ano_final = int(today.strftime('%Y'))
    mes_final = int(today.strftime('%m'))

    for ano in range(ano_inicial, ano_final+1):
        for mes in range(1, 13):
            # evita pegar anos futuros, visto que o arquivo ainda não existe
            if ano == ano_final and mes > mes_final:
                break

            mes = str(mes).zfill(2)

            processa_arquivo(mes, ano)

    return True


def processa_arquivo(mes, ano):
    url_base = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{}{}.csv'
    url = url_base.format(ano, mes)
    print(url)

    try:
        df = pd.read_csv(
            url,
            sep=';',
            encoding='latin1'
        )
    except Exception:
        print('Erro ao baixar arquivo', url)
        return False

    # transforma o campo CO_PRD
    df['CO_PRD'] = df['CNPJ_FUNDO'].str.replace('.', '')
    df['CO_PRD'] = df['CO_PRD'].str.replace('/', '')
    df['CO_PRD'] = df['CO_PRD'].str.replace('-', '')
    df['CO_PRD'] = df['CO_PRD'].str.zfill(14)

    df['DT_COMPTC'] = pd.to_datetime(
        df['DT_COMPTC'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_REF'] = df['DT_COMPTC']

    engine = create_engine('sqlite:///data.sqlite', echo=False)
    sqlite_connection = engine.connect()

    # lê a tabela do banco para juntar os dados quando necessário
    df_db = pd.read_sql_table('swdata', sqlite_connection)

    # faz o concatenar com a base do banco
    df_keys = pd.concat([df_db, df], keys=['CO_PRD', 'DT_REF'])

    # remove duplicados
    df_keys.drop_duplicates(
        keep='last',
        inplace=True,
        ignore_index=True,
        subset=['CO_PRD', 'DT_REF']
    )

    # retira os dataframes da memoria
    del df_db
    del df

    print('Importando usando pandas to_sql')
    df_keys.to_sql(
        'swdata',
        sqlite_connection,
        if_exists='replace',
        index=False,
        chunksize=50000
    )

    print(f'{len(df_keys)} Registros importados com sucesso')
    return True


if __name__ == '__main__':
    main()
