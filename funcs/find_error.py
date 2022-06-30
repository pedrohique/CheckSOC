import pyodbc
from datetime import datetime, timedelta
import cryptocode
import configparser
import logging


config = configparser.ConfigParser()
config.read('config.ini')

logging.basicConfig(filename='logFile_relat.log', level=logging.DEBUG, filemode='w+',
                    format='%(asctime)s - %(levelname)s:%(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

class FindErrorSOC:
    def __init__(self):
        '''Dados'''
        self.data: str = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.envios_error = []
        self.cribs_aga = list(range(200,206))
        self.cribs_novon = list(range(65,69))

        '''statisticas'''
        self.coun_aga_ok = 0
        self.coun_novon_ok = 0
        self.coun_aga_bad = 0
        self.coun_novon_bad = 0


        '''CNX DB'''
        server = config.get('dados_banco', 'server')
        port = config.get('dados_banco', 'port')
        database = config.get('dados_banco', 'database')
        uid = config.get('dados_banco', 'uid')
        pwd = config.get('dados_banco', 'pwd')

        uid = cryptocode.decrypt(uid, "i9brgroup")
        pwd = cryptocode.decrypt(pwd, "i9brgroup")
        try:
            self.cnxn = pyodbc.connect(
                f'DRIVER=SQL Server;SERVER={server};PORT={port};DATABASE={database};UID={uid};PWD={pwd};')
            self.cursor = self.cnxn.cursor()
            logging.info('conexão com o banco de dados efetuada com sucesso.')

        except:
            logging.error('não foi possivel conectar ao banco de dados')

        self.consulta_erros()
        self.count_erros()

    def consulta_erros(self):
        """Metodo busca todos os dados da tabela intsoc pela data de ontem"""

        self.cursor.execute(f"SELECT * FROM IntSoc WHERE "
                            f"DtEnvioSoc BETWEEN CONVERT(datetime, '{self.data}T00:00:00') AND "
                            f"CONVERT(datetime, '{self.data}T23:59:59');")
        self.envios_error = self.cursor.fetchall()

    def count_erros(self):
        for trans in self.envios_error:
            if int(trans[7]) in self.cribs_aga:
                if trans[10] == 2:
                    self.coun_aga_bad += 1
                elif trans[10] == 1:
                    self.coun_aga_ok += 1


            elif int(trans[7]) in self.cribs_novon:
                if trans[10] == 2:
                    self.coun_novon_bad += 1
                elif trans[10] == 1:
                    self.coun_novon_ok += 1
            else:
                print(trans)
        print(f'transações AGA ok: {self.coun_aga_ok} \n'
              f'transações AGA bad: {self.coun_aga_bad} \n'
              f'{round((self.coun_aga_bad/self.coun_aga_ok)*100)}% \n'
              f'transações NOVO ok: {self.coun_novon_ok} \n'
              f'transações NOVO bad: {self.coun_novon_bad} \n'
              f'{round((self.coun_novon_bad/self.coun_novon_ok)*100)}% \n')

        """Precisa agora fazer ele ter noção de qual foi o erro e ver como corrigi-lo"""