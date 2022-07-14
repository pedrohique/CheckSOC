# encoding: utf-8
import cryptocode  # descriptografa a senho do config file
import configparser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from bs4 import BeautifulSoup
import logging


config = configparser.ConfigParser()
config.read('config.ini')

logging.basicConfig(filename='logFile_relat.log', level=logging.DEBUG, filemode='w+',
                    format='%(asctime)s - %(levelname)s:%(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

class SendMail:

    def __init__(self, emails, data, results, relat_name, nome_empresa):
        key = 'i9brgroup'
        password_cript = config.get('enviar_email', 'password')
        password = cryptocode.decrypt(password_cript, key)

        self.emails = emails
        self.host = config.get('enviar_email', 'server')
        self.port = config.get('enviar_email', 'port')
        self.user = config.get('enviar_email', 'user')
        self.password = password
        self.data = data
        self.results = results  # dicionario com os resultados
        self.relat_name = relat_name
        self.nome_empresa = nome_empresa

        '''email data'''
        self.soup = None
        self.con = None
        self.email_msg = None

        if type(self.emails) != list:
            self.emails = self.emails.split(',')

        self.Change_html()
        self.connect()
        self.body()
        self.send()

    def Change_html(self):
        logging.info('Alterando HTML base')
        arquivo = config.get('enviar_email', 'html_caminho')

        with open(arquivo, encoding='utf-8') as arc:
            self.soup = BeautifulSoup(arc, "html.parser", from_encoding=["latin-1", "utf-8"])
            self.soup.data.replace_with(str(self.data))
            self.soup.data1.replace_with(str(self.data))
            self.soup.empresa.replace_with(str(self.nome_empresa))

            self.soup.qtd_erros_send.replace_with(str(self.results['qtd_send']))
            self.soup.qtd_trans_db.replace_with(str(self.results['qtd_nosend']))
            self.soup.porcentagem_envio.replace_with(str(self.results['porcentagem_send']))

            self.soup.qtd_receive.replace_with(str(self.results['qtd_received']))
            self.soup.qtd_erros_receive.replace_with(str(self.results['qtd_noreceived']))
            self.soup.porcentagem_envio.replace_with(str(self.results['porcentagem_received']))

            # self.results = self.results.to_html(index=False)  # converte dict em html
            # soup_df = BeautifulSoup(self.results, 'html.parser')  # converte html em soup
            # self.soup.resumo.append(soup_df)
            self.soup = self.soup.decode()


    def connect(self):  # conect
        logging.info('Conectando no servidor')
        self.con = smtplib.SMTP(self.host, self.port)
        self.con.login(self.user, self.password)

    def body(self):  # edita o email
        logging.info('Criando corpo do email')
        message = self.soup
        self.email_msg = MIMEMultipart()
        self.email_msg['From'] = self.user
        self.email_msg['To'] = ','.join(
                self.emails)  # o problema de passar emails em lista é o cabeçalho que só aceita strings
        self.email_msg['Subject'] = f'RELATORIO INTERNO DE INTEGRAÇÃO SOC - {self.data}'
        self.email_msg.attach(MIMEText(message, 'html'))

    def send(self):  # envia o email
        logging.info('Enviando email')
        for i in self.relat_name:
            # filename = i  # falta renomear o arquivo automaticamente
            name_arq = i.split('\\')

            attachment = open(i, 'rb')  # falta renomear o arquivo automaticamente
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % name_arq[1])
            self.email_msg.attach(part)
            attachment.close()

        de = self.email_msg['From']
        to = self.emails
        self.con.sendmail(de, to, self.email_msg.as_string())
        self.con.quit()
        logging.info('Email enviado com sucesso.')

