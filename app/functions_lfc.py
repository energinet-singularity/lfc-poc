from datetime import datetime


def add_to_log(message: str):
    """Prints message to terminal with timestamp.

    Arguments:
        message (str): log message

    Returns:
        None
    """
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], message)


def print_lfc_logo():
    """
    Prints a ASCII-art LFC logo to terminal.
    """
    lfc_logo = """
     ▄            ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄
    ▐░▌          ▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌
    ▐░▌          ▐░█▀▀▀▀▀▀▀▀▀ ▐░█▀▀▀▀▀▀▀▀▀
    ▐░▌          ▐░▌          ▐░▌
    ▐░▌          ▐░█▄▄▄▄▄▄▄▄▄ ▐░▌
    ▐░▌          ▐░░░░░░░░░░░▌▐░▌
    ▐░▌          ▐░█▀▀▀▀▀▀▀▀▀ ▐░▌
    ▐░▌          ▐░▌          ▐░▌
    ▐░█▄▄▄▄▄▄▄▄▄ ▐░▌          ▐░█▄▄▄▄▄▄▄▄▄
    ▐░░░░░░░░░░░▌▐░▌          ▐░░░░░░░░░░░▌
     ▀▀▀▀▀▀▀▀▀▀▀  ▀            ▀▀▀▀▀▀▀▀▀▀▀
    """
    print(lfc_logo)
