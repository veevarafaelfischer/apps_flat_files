import getopt
import sys
import requests
import unidecode as unidecode

MESSAGE_HELP_PARAMETERS = "SlackMessage.py -canal <idcanal [1 - ERROR | 2 - SUCCESS]> -m <message>"
SLACK_URL_NOTIFICATION_ERROR = "https://hooks.slack.com/services/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"
SLACK_URL_NOTIFICATION_SUCCESS = "https://hooks.slack.com/services/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"
GOOGLE_CHAT_URL_NOTIFICATION_TESTES = "https://chat.googleapis.com/v1/spaces/AAAAWbSAoP0/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=o7cjvCaXEgRIkOqIqbxhIqY08xqdFBHftXg3Asakg6c%3D"
GOOGLE_CHAT_URL_NOTIFICATION_DEBUG = "https://chat.googleapis.com/v1/spaces/AAAAukzAyKo/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=kA9qxwEQ9GsSQh3CZystAQ9GjvbpRcoYDyvWRfMlR_Y%3D"
GOOGLE_CHAT_URL_NOTIFICATION_ERROR = "https://chat.googleapis.com/v1/spaces/AAAAAjcrv1A/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=-iX37gh2J85Al6jTevly5WMPH9ij2md4oHGz_v2VBzo%3D"
GOOGLE_CHAT_URL_NOTIFICATION_SUCCESS = "https://chat.googleapis.com/v1/spaces/AAAAAjcrv1A/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=gOxvTtonSCOvXNqfk6q_XVhFLYtShCPQRwIrYylpVV4%3D"
GOOGLE_CHAT_URL_NOTIFICATION_OPENDATA_TASKS_SUCCESS = "https://chat.googleapis.com/v1/spaces/AAAAUAhDvLc/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=YOAXGqvcDtIKFGd8udnSUKQP-YZTiuIEwAgiJyzM5AE"


def send_slack_message(canal, message):
    if canal in ("-1", "DEBUG", GOOGLE_CHAT_URL_NOTIFICATION_DEBUG):
        url_notification = GOOGLE_CHAT_URL_NOTIFICATION_DEBUG
    elif canal in ("1", "ERROR", SLACK_URL_NOTIFICATION_ERROR):
        url_notification = SLACK_URL_NOTIFICATION_ERROR
    elif canal in ("2", "SUCCESS", SLACK_URL_NOTIFICATION_SUCCESS):
        url_notification = SLACK_URL_NOTIFICATION_SUCCESS
    else:
        print('Canal invalido')
        sys.exit(3)

    payload = '{"text": "%s"}' % message
    response = requests.post(url=url_notification, data=payload)
    resultado = response.text
    print(resultado)


def send_google_chat_message(canal, message):
    if canal in ["-1", "DEBUG", GOOGLE_CHAT_URL_NOTIFICATION_TESTES]:
        url_notification = GOOGLE_CHAT_URL_NOTIFICATION_TESTES
        # url_notification = GOOGLE_CHAT_URL_NOTIFICATION_DEBUG
        message = 'DEBUG => ' + message
    elif canal in ["1", "ERROR", GOOGLE_CHAT_URL_NOTIFICATION_ERROR]:
        url_notification = GOOGLE_CHAT_URL_NOTIFICATION_ERROR
    elif canal in ["2", "SUCCESS", GOOGLE_CHAT_URL_NOTIFICATION_SUCCESS]:
        url_notification = GOOGLE_CHAT_URL_NOTIFICATION_SUCCESS
    elif canal in ["3", "TASK_SUCCESS", GOOGLE_CHAT_URL_NOTIFICATION_OPENDATA_TASKS_SUCCESS]:
        url_notification = GOOGLE_CHAT_URL_NOTIFICATION_OPENDATA_TASKS_SUCCESS
    else:
        print('Canal invalido')
        sys.exit(3)

    message = remove_accents(message)
    payload = '{"text": "%s"}' % message
    headers_notification = {'Content-Type': 'application/json; charset=UTF-8'}
    response = requests.post(url=url_notification, data=payload, headers=headers_notification)
    resultado = response.text
    print(resultado)


def remove_accents(string):
    return unidecode.unidecode(string)

def main(argv):
    message = ''
    canal = 0

    try:
        opts, args = getopt.getopt(argv, "c:m:h", ["canal=", "message="])
    except getopt.GetoptError:
        print(MESSAGE_HELP_PARAMETERS)
        sys.exit(2)

    if len(opts) == 0:
        print("Nenhum parametro informado")
        sys.exit(1)

    for opt, arg in opts:
        opt = opt.lower()

        if opt in ('-h', '--help'):
            print(MESSAGE_HELP_PARAMETERS)
            sys.exit()
        elif opt in ('-c', '--canal'):
            canal = arg
        elif opt in ('-m', '--message'):
            message = arg

    # send_slack_message(canal, message)
    send_google_chat_message(canal, message)
    sys.exit(0)

if __name__ == "__main__":
    main(sys.argv[1:])