import pyodbc
from datetime import datetime, timedelta
import cryptocode
import configparser
import logging
import pandas as pd

config = configparser.ConfigParser()
config.read('config.ini')

logging.basicConfig(filename='logFile_relat.log', level=logging.DEBUG, filemode='w+',
                    format='%(asctime)s - %(levelname)s:%(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

class FindErrorSOC:
    def __init__(self, cribs, empresa):
        '''Dados'''
        self.data: str = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.envios_dict = []  # dicionario de transações que foram enviadas
        self.trans_dict = []  # Dicionario de transações que deveriam ser enviadas
        self.nome_arquivos = []  # Nomes dos arquivos que serão enviados
        self.Dados_NoSend = None  # Dados que não foram enviados
        self.erros_recebimento = None  # Dados que não foram recebidos
        if len(cribs) > 1:
            self.cribs_tuple = tuple(cribs)  # cria uma tupla de cribs atraves de uma lista de cribs
        else:
            self.cribs_tuple = (0, cribs[0])
        self.empresa = empresa.replace(' ', '')

        '''tatisticas'''
        self.numeros = {}
        self.erros_dirty_number = 0 #lista de erros sem tratamento




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
        self.analisa_envio() # analisa o envio primeiro, pois precisamos
        # tratar os dados que não deveriam ser enviados e deram erros para não haver divergencia de numeros
        self.valida_dados()
        self.valida_envio()
        self.revalida_dados()
        self.cria_arquivos()

    def consulta_erros(self):
        """Metodo busca todos os dados da tabela intsoc pela data de ontem"""
        logging.info('Buscando transações de envio ao soc')

        self.cursor.execute(f"SELECT * FROM IntSoc WHERE "
                            f"DtEnvioSoc BETWEEN CONVERT(datetime, '{self.data}T00:00:00') AND "
                            f"CONVERT(datetime, '{self.data}T23:59:59') AND Crib in {self.cribs_tuple} ;")
        envios = self.cursor.fetchall()
        columnNames = [column[0] for column in self.cursor.description]
        for erro in envios:
            self.envios_dict.append(dict(zip(columnNames, erro)))

    def consulta_bd(self):
        """Busca todas as transações no banco de dados para comparar com o enviado pelo serviço"""
        logging.info('Buscando informações no banco de dados')
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
        columnNames = [column[0] for column in self.cursor.description]
        for i in all_trans:
            self.trans_dict.append(dict(zip(columnNames, i)))

    def revalida_dados(self):
        logging.info('Revalidando dados de envio')
        for trans in self.trans_dict:
            validacao = filter(lambda envio: envio['Transnumber'] == trans['transnumber'], self.envios_dict)
            if list(validacao):  # se esta na lista de transações esta validada
                pass
            else:
                logging.warning(f'envio falho: {trans}')

        for trans in self.envios_dict:
            validacao = filter(lambda envio: envio['transnumber'] == trans['Transnumber'], self.trans_dict)
            if list(validacao):  # se esta na lista de transações esta validada
                pass
            else:
                logging.warning(f'envio falho: {trans}')




    def valida_dados(self):
        logging.info('Deparando dados de envio')
        logging.info(f'dados enviados: {len(self.envios_dict)}, dados no banco: {len(self.trans_dict)}')
        if len(self.envios_dict) != len(self.trans_dict):
            logging.warning('Dados de envio divergente')

        for trans in self.trans_dict:
            validacao = filter(lambda envio: envio['Transnumber'] == trans['transnumber'], self.envios_dict)
            if list(validacao):  # se esta na lista de transações esta validada
                trans['status'] = True
            else:
                trans['status'] = False

    def valida_envio(self):
        logging.info('Validando dados de envio')
        '''Compara dados do banco com os dados enviados pela API'''
        dados_true = list(filter(lambda banco: banco['status'] == True, self.trans_dict))
        dados_false = list(filter(lambda banco: banco['status'] == False, self.trans_dict))

        QtdDadosSend = len(dados_true) - self.erros_dirty_number
        QtdDadosNoSend = len(dados_false)

        if QtdDadosSend != 0:
            PercentSend = (100 - round(QtdDadosNoSend/(QtdDadosSend/100)))
        else:
            PercentSend = 0

        self.numeros['qtd_send'] = QtdDadosSend
        self.numeros['qtd_nosend'] = QtdDadosNoSend
        self.numeros['porcentagem_send'] = PercentSend

        if dados_false:
            self.Dados_NoSend = pd.DataFrame.from_dict(dados_false)

        logging.info(f'Empresa: {self.empresa} ---Itens Enviados: {QtdDadosSend} -- '
                     f'Itens não enviados: {QtdDadosNoSend} -- Porcentagem: {PercentSend}')

    def analisa_envio(self):
        def trata_erros(self, erros):
            erros_limpos = []
            if self.empresa == 'AngloGold':
                for erro in erros:
                    # print('--------------------------------------')
                    # print(erro)
                    id = list(filter(lambda banco: banco['EmployeeLocalID'] == erro['IssuedTo']
                                                   and banco['transnumber'] == erro['Transnumber'], self.trans_dict))
                    # print(id)
                    # print('--------------------------------------')
                    try:
                        if id[0]['IssuedTo'].startswith('200') and not id[0]['IssuedTo'].startswith('200105'):
                            erros_limpos.append(erro)
                    except:
                        logging.info(f'Transação não encontrada no Banco - {erro}')
                return erros_limpos
            else:
                return erros


        logging.info('Analisando dados de envio')
        '''Filtra erros de recebimento SOC'''
        erros_dirty = list(filter(lambda envio: envio['EnvioSoc'] == 2, self.envios_dict))
        acertos = list(filter(lambda envio: envio['EnvioSoc'] == 1, self.envios_dict))
        erros = trata_erros(self, erros_dirty)

        self.erros_dirty_number = len(erros_dirty) - len(erros)

        QtdDadosRecebidos = len(acertos)
        QtdDadosNaoRecebidos = len(erros)
        if QtdDadosRecebidos > 0:
            PercentRecebido = 100 - round(QtdDadosNaoRecebidos/(len(self.envios_dict)/100))
        else:
            PercentRecebido = 0

        self.numeros['qtd_received'] = QtdDadosRecebidos
        self.numeros['qtd_noreceived'] = QtdDadosNaoRecebidos
        self.numeros['porcentagem_received'] = PercentRecebido



        logging.info(f'Empresa: {self.empresa} --- Itens recebidos: {QtdDadosRecebidos} '
                     f'-- Itens não recebidos: {QtdDadosNaoRecebidos} -- Porcentagem de '
                     f'Recebimento: {PercentRecebido}')

        if erros:
            self.erros_recebimento = pd.DataFrame.from_dict(erros).sort_values('Erro', ignore_index=True)


    def cria_arquivos(self):
        logging.info('Criando arquivos')
        if self.erros_recebimento is not None:
            nome_arquivo_recebimento = f'arquivos\\erros_recebimento{self.empresa}-{self.data}.xlsx'
            self.erros_recebimento.to_excel(nome_arquivo_recebimento, index=False)
            self.nome_arquivos.append(nome_arquivo_recebimento)
        if self.Dados_NoSend is not None:
            nome_arquivo_envio = f'arquivos\\erros_envio{self.empresa}-{self.data}.xlsx'
            self.Dados_NoSend.to_excel(nome_arquivo_envio, index=False)
            self.nome_arquivos.append(nome_arquivo_envio)
        logging.info('Arquivos criados com sucesso.')

