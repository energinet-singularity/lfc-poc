# Import dependencies
from time import sleep
from json import dumps
import os
import xmltodict

# Import functions
from functions_kafka_class import KafkaHelper
from functions_lfc import add_to_log

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm

xml_last_modified = 0
xml_filepath = "data/" + "lmol.xml"

if __name__ == "__main__":

    # Create lists of topic names produced to and consumed from
    topics_produced_list = [tp_nm.lfc_bsp_lmol]

    # init kafka
    kafka_obj = KafkaHelper(topics_produced_list=topics_produced_list)

    while True:
        sleep(1)

        xml_modified = os.path.getmtime(xml_filepath)

        if xml_last_modified != xml_modified:
            add_to_log(f"Info: Reading bid list from: '{xml_filepath}''.")

            xml_data = open(xml_filepath, 'r').read()
            dict_data = xmltodict.parse(xml_data)
            bid_list = dict_data["list"]["bid"]

            kafka_obj.produce_message(topic_name=tp_nm.lfc_bsp_lmol,
                                      msg_value={msg_val_nm.lfc_bsp_lmol: dumps(bid_list)})

            xml_last_modified = xml_modified
            add_to_log("Info: Read bid list and send as message to Kafka.")
