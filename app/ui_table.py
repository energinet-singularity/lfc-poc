# Import depedencies
from prettytable import PrettyTable
from time import sleep, time
from datetime import datetime
from json import loads
import os
import logging

# Import functions
from singukafka import KafkaHelper

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm

# Initialize log
log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-4s %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S.%03d')
logging.getLogger().setLevel(logging.WARNING)

UI_REFRESH_RATE = 1

# init tables
lfc_table = PrettyTable()
lfc_table.field_names = ["", "Value [MW]", "Description"]

bid_table = PrettyTable()
bid_table.field_names = ["mrid_bsp", "mrid_bid", "name", "direction", "price", "capacity", "availiable"]

bsp_table = PrettyTable()
bsp_table.field_names = ["BSP", "Setpoint [MW]"]


# art
LFC_LOGO = """
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


if __name__ == "__main__":

    # Create lists of topic names produced to and consumed from
    topics_consumed_list = [tp_nm.lfc_p_dem, tp_nm.lfc_p_corr, tp_nm.lfc_p_input,
                            tp_nm.lfc_p_target, tp_nm.lfc_pbr_response, tp_nm.lfc_mw_diff,
                            tp_nm.lfc_bsp_lmol,
                            tp_nm.lfc_bsp_activated]

    # init kafka
    kafka_obj = KafkaHelper(group_id=None,
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                            topics_consumed_list=topics_consumed_list)

    while True:

        sleep(UI_REFRESH_RATE)

        # time of loop start
        time_loop_start = time()

        lfc_table.clear_rows()
        bid_table.clear_rows()
        bsp_table.clear_rows()

        # get latest messages from consumed topics
        msg_val_dict = kafka_obj.get_latest_topic_messages_to_dict_poll_based()

        # LFC data
        table_row_dict = [{"tp_nm": tp_nm.lfc_pbr_response,
                           "msg_val_nm": msg_val_nm.lfc_pbr_response,
                           "NAME": "PBR response",
                           "DESCP": "Simulated PBR response to P_Target"},
                          {"tp_nm": tp_nm.lfc_mw_diff,
                           "msg_val_nm": msg_val_nm.lfc_mw_diff,
                           "NAME": "MW difference",
                           "DESCP": "Diffence between production and consumption"},
                          {"tp_nm": tp_nm.lfc_p_dem,
                           "msg_val_nm": msg_val_nm.lfc_p_dem,
                           "NAME": "P_Demand",
                           "DESCP": "Demand (Simulated as: pbr response + MW_diff)"},
                          {"tp_nm": tp_nm.lfc_p_corr,
                           "msg_val_nm": msg_val_nm.lfc_p_corr,
                           "NAME": "P_Correction",
                           "DESCP": "Correction signal (ie. from PICASSO)"},
                          {"tp_nm": tp_nm.lfc_p_input,
                           "msg_val_nm": msg_val_nm.lfc_p_input,
                           "NAME": "P_Input",
                           "DESCP": "P_demand + P_Correction"},
                          {"tp_nm": tp_nm.lfc_p_target,
                           "msg_val_nm": msg_val_nm.lfc_p_target,
                           "NAME": "P_Target",
                           "DESCP": "PID controller calculated, based on P_input."}]

        for row in table_row_dict:
            if "tp_nm" in row:
                if msg_val_dict[row["tp_nm"]] is None:
                    msg_val = "N/A"
                else:
                    msg_val = msg_val_dict[row["tp_nm"]][row["msg_val_nm"]]
                lfc_table.add_row([row["NAME"], msg_val, row["DESCP"]])

        # LMOL data
        if msg_val_dict[tp_nm.lfc_bsp_lmol] is not None:
            data_lmol = loads(msg_val_dict[tp_nm.lfc_bsp_lmol][msg_val_nm.lfc_bsp_lmol])
            data_lmol.sort(key=lambda x: (x['direction'], float(x['price'])), reverse=False)

            # Fill table with bids from LMOL
            for bid in data_lmol:
                # TODO loop list og lav table der ud fra
                bid_table.add_row([bid["mrid_bsp"],
                                  bid["mrid_bid"],
                                  bid["name"],
                                  bid["direction"],
                                  bid["price"],
                                  bid["capacity"],
                                  bid["availiable"]])

        # BSP data
        if msg_val_dict[tp_nm.lfc_bsp_activated] is not None:
            data_bsp = msg_val_dict[tp_nm.lfc_bsp_activated][msg_val_nm.lfc_bsp_activated]
            data_bsp.sort(key=lambda x: x['mrid'], reverse=False)

            # Fill table
            for key in data_bsp:
                bsp_table.add_row([key["mrid"], key["setpoint"]])

        lfc_table.align = "l"
        lfc_table.junction_char

        bid_table.align = "l"
        bid_table.junction_char

        bsp_table.align = "l"
        bsp_table.junction_char

        # os.system('cls' if os.name == 'nt' else 'clear')

        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}: took {round((time()-time_loop_start),3)} to get data." +
              f"{LFC_LOGO}" +
              f"- Avaliable bids from LMOL -\n" +
              f"{lfc_table}" +
              f"\n- Avaliable bids from LMOL -\n" +
              f"{bid_table}" +
              f"\n- Activated BSP's based on bids -\n" +
              f"{bsp_table}")