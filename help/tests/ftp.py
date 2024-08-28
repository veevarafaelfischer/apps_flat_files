from os import path, stat
from ftplib import FTP_TLS, FTP

import paramiko
import pysftp

from _public.shared.sql import SQL
from _public.shared.utils import *

ftp_host = "nbpx.daiichisankyo.com.br"
ftp_port = "22"
ftp_user = "veeva_sftp_r"
ftp_pwd = "GHIwdBn!"


def ftp_tls():
	try:
		ftp = FTP_TLS()
		ftp.port(ftp_port)
		ftp.prot_p()	# Método relacionado a segurança TSL
		print('FTP - Host encontrado: ', ftp_host)

		ftp.login(user=ftp_user, passwd=ftp_pwd)
		print('FTP - Login efetuado com sucesso.')

		ftp.quit()
		print('FTP - Conexao fechada com sucesso')

	except Exception as ex:
		print(ex)


def py_ftp():
	ftp = pysftp.Connection(
		host=ftp_host,
		username=ftp_user,
		password=ftp_pwd,
		port=ftp_port,
	)

	arr_folders = ftp.listdir('.')
	ftp.close()



def ftp_paramiko():
	try:
		client = paramiko.SSHClient()
		client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		client.connect(hostname=ftp_host, port=ftp_port, username=ftp_user, password=ftp_pwd, allow_agent=False, look_for_keys=False)
		sftp = client.open_sftp()

		for dir in sftp.listdir('.'):
			print(dir)
		print('Conexão realizada com sucesso!')

		sftp.close()

	except Exception as ex:
		print('Erro na conexao: ', str(ex))

# ftp_tls()
py_ftp()
# ftp_paramiko()


exit(0)
