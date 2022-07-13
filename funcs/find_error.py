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
        self.envios_dict = []
        self.trans_dict = []
        self.cribs_aga = list(range(200, 206))
        self.cribs_novon = list(range(65, 69))
        self.cribs_tuple = tuple(self.cribs_novon)
        print(self.cribs_tuple)

        '''statisticas'''
        self.dict_numeros = {}

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
        self.consulta_bd()
        self.valida_dados()
        # self.count_erros()

    def consulta_erros(self):
        """Metodo busca todos os dados da tabela intsoc pela data de ontem"""


        self.cursor.execute(f"SELECT * FROM IntSoc WHERE "
                            f"DtEnvioSoc BETWEEN CONVERT(datetime, '{self.data}T00:00:00') AND "
                            f"CONVERT(datetime, '{self.data}T23:59:59');")
        envios = self.cursor.fetchall()
        columnNames = [column[0] for column in self.cursor.description]
        print(columnNames)
        for erro in envios:
            self.envios_dict.append(dict(zip(columnNames, erro)))

    def consulta_bd(self):
        self.cursor.execute(f"SELECT INVENTRY.Description1,  INVENTRY.Description2,  "
                            f"INVENTRY.MfrNumber,  EMPLOYEE.EmployeeLocalID, "
                            f"EMPLOYEE.EmployeeSiteID, TRANS.Transdate, TRANS.transnumber, "
                            f"TRANS.Crib, TRANS.Item, TRANS.CribBin, TRANS.quantity, "
                            f"TRANS.TypeDescription, TRANS.IssuedTo "
                            f"FROM ((TRANS TRANS "
                            f"LEFT OUTER JOIN EMPLOYEE EMPLOYEE ON TRANS.IssuedTo=EMPLOYEE.ID) "
                            f"LEFT OUTER JOIN INVENTRY INVENTRY ON TRANS.Item=INVENTRY.ItemNumber) "
                            f"WHERE "
                            f"Crib in {self.cribs_tuple} AND "
                            f"TRANS.TypeDescription='ISSUE' AND "
                            f"Transdate BETWEEN CONVERT(datetime, '{self.data}T00:00:00') AND "
                            f"CONVERT(datetime, '{self.data}T23:59:59') AND "
                            f"Status is Null;")

        all_trans = self.cursor.fetchall()
        trans_list = []
        columnNames = [column[0] for column in self.cursor.description]
        print(columnNames)
        for i in all_trans:
            self.trans_dict.append(dict(zip(columnNames, i)))


    def valida_dados(self):
        if len(self.trans_dict) >= len(self.envios_dict):
            print('hi', len(self.trans_dict), len(self.envios_dict))
            for trans in self.trans_dict:
                print(trans['transnumber'])
                teste = filter(lambda envio: envio['Transnumber'] == trans['transnumber'], self.envios_dict)
                if list(teste):  # se esta na lista de transações esta validada
                    print('ok')
                else:
                    trans['status'] = False
                    print(trans['EmployeeSiteID'])
            print('if')

        else:
            for trans in self.envios_dict:
                print(trans['Transnumber'])
                teste = filter(lambda envio: envio['transnumber'] == trans['Transnumber'], self.trans_dict)
                if list(teste):  # se esta na lista de envios esta validada
                    print('ok')
                else:
                    trans['status'] = False
            print('else')



    # def count_erros(self):
    #     coun_aga_bad = 0
    #     coun_aga_ok = 0
    #     coun_novon_bad = 0
    #     coun_novon_ok = 0
    #
    #     for trans in self.envios_dict:
    #         if int(trans['Crib']) in self.cribs_aga:
    #             if trans['EnvioSoc'] == 2:
    #                 coun_aga_bad += 1
    #             elif trans['EnvioSoc'] == 1:
    #                 coun_aga_ok += 1
    #
    #
    #         elif int(trans['Crib']) in self.cribs_novon:
    #             if trans['EnvioSoc'] == 2:
    #                 coun_novon_bad += 1
    #             elif trans['EnvioSoc'] == 1:
    #                 coun_novon_ok += 1
    #         else:
    #             print(trans)
    #
    #     print(f'transações AGA ok: {coun_aga_ok} \n'
    #           f'transações AGA bad: {coun_aga_bad} \n'
    #           #f'{round((self.coun_aga_bad/self.coun_aga_ok)*100)}% \n'
    #           f'transações NOVO ok: {coun_novon_ok} \n'
    #           f'transações NOVO bad: {coun_novon_bad} \n')
    #           #f'{round((self.coun_novon_bad/self.coun_novon_ok)*100)}% \n')

    """Precisa agora fazer ele ter noção de qual foi o erro e ver como corrigi-lo"""