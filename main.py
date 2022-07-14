"""Objetivo desse projeto é enviar alertas referente a integração i9 -> SOC"""
from funcs import find_error
from funcs import send_mail

from datetime import datetime, timedelta, date
import time
import logging
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read('config.ini')

logging.basicConfig(filename='logFile_relat.log', level=logging.DEBUG, filemode='w+',
                    format='%(asctime)s - %(levelname)s:%(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

def trata_cribs(cribs_sujos):
    cribs = []
    if type(cribs_sujos) == str:
        cribs_sujos = cribs_sujos.split('-')
        cribs_sujos = range(int(cribs_sujos[0]), int(cribs_sujos[1])+1)
        for crib in cribs_sujos:
            cribs.append(crib)
    else:
        cribs.append(int(cribs_sujos))

    return cribs

nome_arquivo = config.get('funcionamento', 'arquivo_de_config')
hora_programada = config.get('funcionamento', 'hora_programada')



if __name__ == '__main__':
    while True:
        hora_atual = datetime.today().strftime('%H:%M')
        print(hora_atual, type(hora_atual), hora_programada, type(hora_programada))
        logging.info(f'consultando hora programada: {hora_programada}')
        if hora_programada == hora_atual:
            '''TREAD'''
            dados = pd.read_excel(nome_arquivo)
            for i in dados.index:
                cribs = dados['cribs-interval'][i]
                cribs = trata_cribs(cribs)
                cribs_name = dados['nome_empr'][i]
                cribs_emails = dados['emails'][i]
                inactive = dados['inactive'][i]
                time.sleep(1)
                if inactive == 0:
                    feSCO = find_error.FindErrorSOC(cribs, cribs_name)
                    send_mail.SendMail(cribs_emails, feSCO.data, feSCO.numeros, feSCO.nome_arquivos, cribs_name)
            time.sleep(60)
        else:
            hora_atual_obj = datetime.strptime(hora_atual, '%H:%M').time()
            hora_programada_obj = datetime.strptime(hora_programada, '%H:%M').time()
            falta = (datetime.combine(date.min, hora_programada_obj) - datetime.combine(date.min,
                                                                                        hora_atual_obj)) / timedelta(
                seconds=1)
            dia = timedelta(days=1) / timedelta(seconds=1)
            if falta < 0:
                falta = dia - (falta * -1)

            falta = int(falta)
            logging.info(f'hora programada invalida. Resta {falta} segundos para execução')
            time.sleep(falta)