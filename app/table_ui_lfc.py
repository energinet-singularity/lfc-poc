# Import depedencies
from prettytable import PrettyTable
from time import sleep
import sys

# Import functions
from functions_kafka import (set_kafka_brooker_from_env, init_consumer, init_topic_partitions,
                             subscribe_topics,
                             get_latest_topic_messages_to_dict_poll_based, get_msg_val_from_dict,
                             list_unavbl_topics, list_empty_topics)
from functions_lfc import (print_lfc_logo_doh, add_to_log)

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm
import parm_general as PARM

lfc_table = PrettyTable()
lfc_table.field_names = ["", "Value [MW]", "Description"]

if __name__ == "__main__":

    # Set Kafka brooker from environment variables
    kafka_brooker = set_kafka_brooker_from_env()

    # Create lists of topic consumed
    topics_consumed_list = [tp_nm.lfc_p_dem, tp_nm.lfc_p_corr, tp_nm.lfc_p_input,
                            tp_nm.lfc_p_target, tp_nm.lfc_pbr_response, tp_nm.lfc_mw_diff]

    # Create dictionary for holding latest message value for each consumed topic
    topic_latest_message_value_dict = {}

    # Initialize Kafka consumer
    consumer_kafka = init_consumer(bootstrap_servers=kafka_brooker,
                                   group_id=None,
                                   auto_offset_reset='earliest',
                                   enable_auto_commit=False)

    # Subscribe to consumed topics
    subscribe_topics(consumer=consumer_kafka, topic_list=topics_consumed_list)

    # Dummy poll needed to force partitions assignment
    init_topic_partitions(consumer=consumer_kafka, topic_list=topics_consumed_list, timeout_ms=PARM.TIMEOUT_MS_POLL)

    # Verify if all needed kafka topics exist
    if list_unavbl_topics(consumer=consumer_kafka, topic_list=topics_consumed_list, print_err=True):
        sys.exit(1)

    while True:

        lfc_table.clear_rows()

        # Get latest value for each comnsumed topic
        topic_latest_message_value_dict = get_latest_topic_messages_to_dict_poll_based(consumer=consumer_kafka,
                                                                                       topic_list=topics_consumed_list,
                                                                                       timeout_ms=PARM.TIMEOUT_MS_POLL)
        # add_to_log(f"Debug: Getting messages took: {round(time()-time_loop_start,3)} secounds.")

        # Report warnings on empty topics
        list_empty_topics(topic_latest_message_value_dict=topic_latest_message_value_dict, print_warn=True)

        # Fill table
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
                msg_val = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                tp_nm=row["tp_nm"],
                                                msg_val_nm=row["msg_val_nm"],
                                                default_val="NA",
                                                precision=PARM.PRECISION_DECIMALS)
                lfc_table.add_row([row["NAME"], msg_val, row["DESCP"]])

        # display table
        print_lfc_logo_doh()
        add_to_log("")
        lfc_table.align = "l"
        lfc_table.junction_char
        print(lfc_table)
        sleep(1)
