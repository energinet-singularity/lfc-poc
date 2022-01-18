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


def print_bsp_logo_doh():
    """
    Prints a ASCII-art BSP logo to terminal.
    """
    logo = """
BBBBBBBBBBBBBBBBB           SSSSSSSSSSSSSSS      PPPPPPPPPPPPPPPPP
B::::::::::::::::B        SS:::::::::::::::S     P::::::::::::::::P
B::::::BBBBBB:::::B      S:::::SSSSSS::::::S     P::::::PPPPPP:::::P
BB:::::B     B:::::B     S:::::S     SSSSSSS     PP:::::P     P:::::P
  B::::B     B:::::B     S:::::S                   P::::P     P:::::P
  B::::B     B:::::B     S:::::S                   P::::P     P:::::P
  B::::BBBBBB:::::B       S::::SSSS                P::::PPPPPP:::::P
  B:::::::::::::BB         SS::::::SSSSS           P:::::::::::::PP
  B::::BBBBBB:::::B          SSS::::::::SS         P::::PPPPPPPPP
  B::::B     B:::::B            SSSSSS::::S        P::::P
  B::::B     B:::::B                 S:::::S       P::::P
  B::::B     B:::::B                 S:::::S       P::::P
BB:::::BBBBBB::::::B     SSSSSSS     S:::::S     PP::::::PP
B:::::::::::::::::B      S::::::SSSSSS:::::S     P::::::::P
B::::::::::::::::B       S:::::::::::::::SS      P::::::::P
BBBBBBBBBBBBBBBBB         SSSSSSSSSSSSSSS        PPPPPPPPPP
    """
    print(logo)

def print_bid_logo_doh():
    """
    Prints a ASCII-art BID logo to terminal.
    """
    logo = """
BBBBBBBBBBBBBBBBB        IIIIIIIIII     DDDDDDDDDDDDD
B::::::::::::::::B       I::::::::I     D::::::::::::DDD
B::::::BBBBBB:::::B      I::::::::I     D:::::::::::::::DD
BB:::::B     B:::::B     II::::::II     DDD:::::DDDDD:::::D
  B::::B     B:::::B       I::::I         D:::::D    D:::::D
  B::::B     B:::::B       I::::I         D:::::D     D:::::D
  B::::BBBBBB:::::B        I::::I         D:::::D     D:::::D
  B:::::::::::::BB         I::::I         D:::::D     D:::::D
  B::::BBBBBB:::::B        I::::I         D:::::D     D:::::D
  B::::B     B:::::B       I::::I         D:::::D     D:::::D
  B::::B     B:::::B       I::::I         D:::::D     D:::::D
  B::::B     B:::::B       I::::I         D:::::D    D:::::D
BB:::::BBBBBB::::::B     II::::::II     DDD:::::DDDDD:::::D
B:::::::::::::::::B      I::::::::I     D:::::::::::::::DD
B::::::::::::::::B       I::::::::I     D::::::::::::DDD
BBBBBBBBBBBBBBBBB        IIIIIIIIII     DDDDDDDDDDDDD
    """
    print(logo)
