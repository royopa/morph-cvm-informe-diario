
# -*- coding: utf-8 -*-
import os
import datetime
import scraperwiki
import pandas as pd
import shutil


def main():
    # morph.io requires this db filename, but scraperwiki doesn't nicely
    # expose a way to alter this. So we'll fiddle our environment ourselves
    # before our pipeline modules load.
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

    today = datetime.date.today()
    ano_inicial = 2019
    ano_final = int(today.strftime('%Y'))
    mes_final = int(today.strftime('%m'))

    for ano in range(ano_inicial, ano_final+1):
        for mes in range(1,13):
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
    df['CO_PRD'] = df['CNPJ_FUNDO'].str.replace('.','')
    df['CO_PRD'] = df['CO_PRD'].str.replace('/','')
    df['CO_PRD'] = df['CO_PRD'].str.replace('-','')
    df['CO_PRD'] = df['CO_PRD'].str.zfill(14)

    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_REF'] = df['DT_COMPTC']

    for row in df.to_dict('records'):
        scraperwiki.sqlite.save(unique_keys=['CO_PRD', 'DT_REF'], data=row)

    print('{} Registros importados com sucesso', len(df))
    return True


if __name__ == '__main__':
    print('Renomeando arquivo sqlite')
    if os.path.exists('data.sqlite'):
        shutil.copy('data.sqlite', 'scraperwiki.sqlite')

    main()

    # rename file
    print('Renomeando arquivo sqlite')
    if os.path.exists('scraperwiki.sqlite'):
        shutil.copy('scraperwiki.sqlite', 'data.sqlite')
