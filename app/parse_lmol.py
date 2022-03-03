# Import dependencies
from time import sleep
from json import dumps
import logging
import os
import xmltodict

# Import functions
from singukafka import KafkaHelper

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm

REFRESH_RATE_PARSE_LMOL = 1

# Initialize log
log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-4s %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S.%03d')
logging.getLogger().setLevel(logging.WARNING)

xml_last_modified = 0
# TODO do not use relative path
xml_filepath = "data/lmol.xml"


if __name__ == "__main__":

    # Create lists of topic names produced to and consumed from
    topics_produced_list = [tp_nm.lfc_bsp_lmol]

    # init kafka
    kafka_obj = KafkaHelper(topics_produced_list=topics_produced_list)

    while True:
        sleep(REFRESH_RATE_PARSE_LMOL)

        xml_modified = os.path.getmtime(xml_filepath)

        if xml_last_modified != xml_modified:
            log.info(f"Reading bid list from: '{xml_filepath}''.")

            """
            TODO add open and close
            xml_file=open(xml_filepath, 'r')
            xml_data = xml_file.read()
            xml_file.close()

            or simply:

            with open(xml_filepath, 'r') as xml_file:
                xml_data = xml_file.read()

            """
            xml_data = open(xml_filepath, 'r').read()
            dict_data = xmltodict.parse(xml_data)

            bid_list = dict_data["list"]["bid"]

            kafka_obj.produce_message(topic_name=tp_nm.lfc_bsp_lmol,
                                      msg_value={msg_val_nm.lfc_bsp_lmol: dumps(bid_list)})

            xml_last_modified = xml_modified
            log.info(f"Read bid list and send as message to Kafka.")
