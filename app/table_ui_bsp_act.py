# Import depedencies
from prettytable import PrettyTable
from time import sleep, time
import sys

# Import functions
from functions_kafka_class import KafkaHelper
from functions_lfc import add_to_log

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm
import parm_general as PARM

bsp_table = PrettyTable()
bsp_table.field_names = ["BSP", "Setpoint [MW]"]

if __name__ == "__main__":

    # Create lists of topic names produced to and consumed from
    topics_consumed_list = [tp_nm.lfc_bsp_activated]

    # init kafka
    kafka_obj = KafkaHelper(group_id=None,
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                            topics_consumed_list=topics_consumed_list,
                            poll_timeout_ms=PARM.TIMEOUT_MS_POLL)

    while True:

        bsp_table.clear_rows()

        # time of loop start
        time_loop_start = time()

        # Report warnings on empty topics ?
        kafka_obj.list_empty_topics()

        # Get latest value for each comnsumed topic
        kafka_obj.get_latest_topic_messages_to_dict_poll_based()

        # get  value
        kafka_obj.get_msg_val_from_dict(tp_nm=tp_nm.lfc_bsp_activated,
                                        msg_val_nm=msg_val_nm.lfc_bsp_activated,
                                        default_val=0)
        data = kafka_obj.message_value
        data.sort(key=lambda x: x['bsp_mrid'], reverse=False)

        # Fill table
        for key in data:
            #print(key)
            bsp_table.add_row([key["bsp_mrid"], key["setpoint"]])
        
        # display table
        # print_lfc_logo_doh()
        add_to_log("")
        bsp_table.align = "l"
        bsp_table.junction_char
        print(bsp_table)
        sleep(1)
