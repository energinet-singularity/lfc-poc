from datetime import datetime
import parm_general as PARM


def add_to_log(message: str):
    """Prints message to terminal with timestamp.

    Arguments:
        message (str): log message

    Returns:
        None
    """
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], message)


def simulate_pbr_response(p_target: float, last_pbr_response: float):
    """ PBR response simulator.

    Simulates the response of PBR.

    Argurments:
        p_target (float): MW target value.
        last_pbr_repsponse (float): last response of simulated PBR response.
    Returns:
        response_pbr (float): simulated MW reponse of PBR.

    """
    if abs(p_target) - (PARM.DEADBAND_PBR_SIMU) < abs(last_pbr_response) < abs(p_target) + (PARM.DEADBAND_PBR_SIMU):
        add_to_log("PBR did not ramp due to deadband.")
        response_pbr = last_pbr_response
    # if ramping down is needed
    elif last_pbr_response > p_target:
        add_to_log("PBR is regulating down.")
        response_pbr = round(last_pbr_response - (PARM.PBR_RAMP_MWS*PARM.REFRESH_RATE_S_LFC_DEM_SIMU), PARM.PRECISION_DECIMALS)
    # if ramping up is needed
    elif last_pbr_response < p_target:
        add_to_log("PBR is regulating up.")
        response_pbr = round(last_pbr_response + (PARM.PBR_RAMP_MWS*PARM.REFRESH_RATE_S_LFC_DEM_SIMU), PARM.PRECISION_DECIMALS)

    return response_pbr


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
