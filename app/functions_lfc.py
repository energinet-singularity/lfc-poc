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


def print_lfc_logo_doh():
    """
    Prints a ASCII-art LFC logo to terminal.
    """
    lfc_logo = """
LLLLLLLLLLL                  FFFFFFFFFFFFFFFFFFFFFF             CCCCCCCCCCCCC
L:::::::::L                  F::::::::::::::::::::F          CCC::::::::::::C
L:::::::::L                  F::::::::::::::::::::F        CC:::::::::::::::C
LL:::::::LL                  FF::::::FFFFFFFFF::::F       C:::::CCCCCCCC::::C
  L:::::L                      F:::::F       FFFFFF      C:::::C       CCCCCC
  L:::::L                      F:::::F                  C:::::C
  L:::::L                      F::::::FFFFFFFFFF        C:::::C
  L:::::L                      F:::::::::::::::F        C:::::C
  L:::::L                      F:::::::::::::::F        C:::::C
  L:::::L                      F::::::FFFFFFFFFF        C:::::C
  L:::::L                      F:::::F                  C:::::C
  L:::::L         LLLLLL       F:::::F                   C:::::C       CCCCCC
LL:::::::LLLLLLLLL:::::L     FF:::::::FF                  C:::::CCCCCCCC::::C
L::::::::::::::::::::::L     F::::::::FF                   CC:::::::::::::::C
L::::::::::::::::::::::L     F::::::::FF                     CCC::::::::::::C
LLLLLLLLLLLLLLLLLLLLLLLL     FFFFFFFFFFF                        CCCCCCCCCCCCC
    """
    print(lfc_logo)
