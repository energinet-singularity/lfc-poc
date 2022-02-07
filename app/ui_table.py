# Import depedencies
from prettytable import PrettyTable
from time import sleep, time
from json import loads

# Import functions
from functions_kafka_class import KafkaHelper
from functions_lfc import add_to_log, print_lfc_logo_doh

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm
import parm_general as PARM

lfc_table = PrettyTable()
lfc_table.field_names = ["", "Value [MW]", "Description"]

bid_table = PrettyTable()
bid_table.field_names = ["mrid_bsp", "mrid_bid", "name", "direction", "price", "capacity", "availiable"]

bsp_table = PrettyTable()
bsp_table.field_names = ["BSP", "Setpoint [MW]"]

if __name__ == "__main__":

    # Create lists of topic names produced to and consumed from
    topics_consumed_list = [tp_nm.lfc_p_dem, tp_nm.lfc_p_corr, tp_nm.lfc_p_input,
                            tp_nm.lfc_p_target, tp_nm.lfc_pbr_response, tp_nm.lfc_mw_diff,
                            tp_nm.lfc_bsp_lmol,
                            tp_nm.lfc_bsp_activated,
    ]

    # init kafka
    kafka_obj = KafkaHelper(group_id="None",
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                            topics_consumed_list=topics_consumed_list,
                            poll_timeout_ms=PARM.TIMEOUT_MS_POLL)

    while True:

        lfc_table.clear_rows()
        bid_table.clear_rows()
        bsp_table.clear_rows()

        # time of loop start
        time_loop_start = time()

        # get latest messages from consumed topics
        msg_val_dict = kafka_obj.get_latest_msg_from_consumed_topics_to_dict()

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
                msg_val = msg_val_dict[row["tp_nm"]][row["msg_val_nm"]]
                """get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                tp_nm=row["tp_nm"],
                                                msg_val_nm=row["msg_val_nm"],
                                                default_val="NA",
                                                precision=PARM.PRECISION_DECIMALS)"""
                lfc_table.add_row([row["NAME"], msg_val, row["DESCP"]])









        # LMOL data
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
        data_bsp = msg_val_dict[tp_nm.lfc_bsp_activated][msg_val_nm.lfc_bsp_activated]
        data_bsp.sort(key=lambda x: x['mrid'], reverse=False)

        # Fill table
        for key in data_bsp:
            bsp_table.add_row([key["mrid"], key["setpoint"]])

        # display tables
        print_lfc_logo_doh()
        
        lfc_table.align = "l"
        lfc_table.junction_char
        print(lfc_table)

        bid_table.align = "l"
        bid_table.junction_char
        print("- Avaliable bids from LMOL -")
        print(bid_table)

        bsp_table.align = "l"
        bsp_table.junction_char
        print("- Activated BSP's based on bids -")
        print(bsp_table)

        add_to_log("")

        sleep(0.5)

