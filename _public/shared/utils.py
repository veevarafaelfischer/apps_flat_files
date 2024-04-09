import os
import _public.shared.SlackMessage as slackMessage


def iif(expressao, verdadeiro, falso):
	return verdadeiro if expressao else falso


def get_files_paths(files_path, is_contain_csv, is_contain_zip):
	arr_files = []
	arr_paths = []

	if is_contain_csv is True:
		arr_paths.append(rf'{files_path}\csv')	# verificar se na config do cliente tem recebimento em csv
	if is_contain_zip is True:
		arr_paths.append(rf'{files_path}\zip')		# verificar se na config do cliente tem recebimento em zip

	for path in arr_paths:
		for file in os.listdir(path):
			arr_files.append(path + '\\' + file)
	return arr_files


def send_notification(mensagem, canal=slackMessage.SLACK_URL_NOTIFICATION_SUCCESS):
	slackMessage.send_google_chat_message(canal=canal, message=mensagem)
