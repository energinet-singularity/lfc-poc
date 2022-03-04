# Import dependencies
from time import sleep, time
import logging
from json import loads

# Import functions
from singukafka import KafkaHelper

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm

# constants
PRECISION_DECIMALS = 2
REFRESH_RATE_CALC_BSP_SETPOINTS = 1

# Initialize log
log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-4s %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S.%03d')
logging.getLogger().setLevel(logging.WARNING)
"""
TODO:
- BSP simulering skal tage sum af setpoints frem for P_target (og noget sum af loop hastigheder?)
- byg visning ind af om bud er aktiv (som ny tabel eller blot visning i act bsp)
- brug kafka helper alle steder
- check if topics which are both produced and consumed are empty, else init (funktion som gør det og melder hvis nogle ikke sættes)
- Dokumenter classes ordentligt
- Lav kafka topics som env vars
- Lav table ui som message loops?
- LMOL via file mount?
"""


class BSP:
    """
    Represents a BSP. Need to be instanciated with a unique MRID.
    The class can be used to hold setpoint value for BSP.

    Attributes
    ----------
    mrid : str
        MRID of BSP.
    setpoint : float, optional
        Value for setpoint for BSP (default is 0).

    Methods
    -------
    set_setpoint(setpoint)
        Set the value for setpoint of the BSP.

    add_to_setpoint(setpoint)
        Add value to the current setpoint of the BSP.
    """
    # init/constructor
    def __init__(self, mrid, setpoint=0):
        """
        Parameters
        ----------
        mrid : str
            MRID of BSP.
        setpoint : float, optional
            Value for setpoint for BSP (default is 0).
        """
        self.mrid = mrid
        self.setpoint = setpoint

    # methods
    # - set setpint
    def set_setpoint(self, setpoint):
        self.setpoint = round(setpoint, PRECISION_DECIMALS)

    # - set setpint
    def add_to_setpoint(self, setpoint):
        self.setpoint += round(setpoint, PRECISION_DECIMALS)


# Bid class blueprint
class Bid:
    # init/constructor
    def __init__(self, mrid_bsp, mrid_bid, name, direction, price, capacity, availiable):
        # attributes
        self.mrid_bsp = mrid_bsp
        self.mrid_bid = mrid_bid
        self.name = name
        self.direction = direction
        self.price = float(price)
        self.capacity = float(capacity)
        self.availiable = bool(availiable)
        self.activated = False

    # methods
    # - activate bid
    def activate_bid(self):
        self.activated = True

    # - deactivate bid
    def deactivate_bid(self):
        self.activated = False


class LMOLHandler():
    def __init__(self, lmol: list, p_target: float):
        self.lmol = lmol
        self.p_target = p_target
        self.bid_list = []
        self.bsp_list = []
        self.bid_candidates = []

        self.init_bid_list()
        self.init_bsp_list()
        self.init_candidata_bids()

    def init_bid_list(self):
        # make list of object with bids
        for bid in self.lmol:
            self.bid_list.append(Bid(mrid_bsp=bid["mrid_bsp"],
                                 mrid_bid=bid["mrid_bid"],
                                 name=bid["name"],
                                 direction=bid["direction"],
                                 price=bid["price"],
                                 capacity=bid["capacity"],
                                 availiable=bid["availiable"]))

    def init_bsp_list(self):
        # make list of object with bsps
        for bid in self.lmol:
            # add bsp to bsp list
            if not any(x.mrid == bid["mrid_bsp"] for x in self.bsp_list):
                self.bsp_list.append(BSP(mrid=bid["mrid_bsp"]))

    def init_candidata_bids(self):
        # init list with candidate bids. Only up or down bids will be included dependent on p_target/regulation need
        # TODO check also aviliable flag or do in other
        if self.p_target > 0:
            # extract up regualtion bids and sort list by cheapest price
            self.bid_candidates = [i for i in self.bid_list if (i.direction == 'UP')]
            self.bid_candidates.sort(key=lambda x: x.price, reverse=False)
            log.info(f"{abs(p_target)} MW up regulation needed.")
        elif self.p_target < 0:
            # extract up regualtion bids and sort list by cheapest price
            self.bid_candidates = [i for i in self.bid_list if (i.direction == 'DOWN')]
            self.bid_candidates.sort(key=lambda x: x.price, reverse=False)
            log.info(f"{abs(p_target)} MW down regulation needed.")
        else:
            log.info(f"No regulation needed. Target is {p_target}.")

    def bsp_func(self):

        # sort bids by either up or down prices dependent on lfc regulation need defined by target

        # mark necessary bids as activated
        # TODO make as function
        if self.bid_candidates:
            availiable_regulation = 0

            remaning_regulation = round(abs(self.p_target), PRECISION_DECIMALS)
            for bid in self.bid_candidates:

                # log.info(f"BSP bid '{bid.mrid_bid}' from {bid.name} neeeded.")
                bid.activate_bid()

                availiable_regulation += bid.capacity
                setpoint = round((min(remaning_regulation, bid.capacity)), PRECISION_DECIMALS)
                remaning_regulation -= setpoint
                # add setpoint to bsp_mrid
                # TODO make smarter
                for bsp in self.bsp_list:
                    if bsp.mrid == bid.mrid_bsp:
                        if self.p_target > 0:
                            bsp.add_to_setpoint(setpoint)
                        elif self.p_target < 0:
                            bsp.add_to_setpoint(-setpoint)

                # if availiable regulation from bids is over targer, then break loop
                if availiable_regulation >= abs(p_target):
                    break

        return self.bsp_list


if __name__ == "__main__":

    # init
    last_p_target = None
    last_lmol = None

    # Create lists of topic names produced to and consumed from
    topics_produced_list = [tp_nm.lfc_bsp_activated]
    topics_consumed_list = [tp_nm.lfc_p_target, tp_nm.lfc_bsp_lmol]

    # init kafka
    consumer_gp_nm = "lfc_bsp_activation"
    kafka_obj = KafkaHelper(group_id=consumer_gp_nm,
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                            topics_consumed_list=topics_consumed_list,
                            topics_produced_list=topics_produced_list)

    log.info("Activating BSP in merit order based on LMOL and P_target..")

    while True:

        # time of loop start
        time_loop_start = time()

        # get latest messages from consumed topics
        msg_val_dict = kafka_obj.get_latest_topic_messages_to_dict_poll_based()
        log.debug(f"Getting messages took: {round(time()-time_loop_start,3)} secounds.")

        # check if consumed only data is availiable and wait if not, else do it
        empty_consumed_only_topics = kafka_obj.list_empty_consumed_only_topics()
        if empty_consumed_only_topics:
            log.warning(f"The consumed only topics: {empty_consumed_only_topics} are empty. Waiting for input data.")
            sleep(REFRESH_RATE_CALC_BSP_SETPOINTS)

        else:
            # Get p target
            p_target = msg_val_dict[tp_nm.lfc_p_target][msg_val_nm.lfc_p_target]

            # get lmol
            lmol_data = msg_val_dict[tp_nm.lfc_bsp_lmol][msg_val_nm.lfc_bsp_lmol]

            # sort lmol
            lmol = loads(lmol_data)
            lmol.sort(key=lambda x: (x['direction'], x['price']), reverse=False)

            # calculate BSP activation of bids if p_target or bid has changed
            if last_p_target != p_target or last_lmol != lmol:

                last_p_target = p_target
                last_lmol = lmol

                lmol_obj = LMOLHandler(lmol=lmol, p_target=p_target)

                bsp_setpoint_list = lmol_obj.bsp_func()

                kafka_obj.produce_message(topic_name=tp_nm.lfc_bsp_activated,
                                          msg_key=msg_val_nm.lfc_bsp_activated,
                                          msg_value={msg_val_nm.lfc_bsp_activated: [bsp.__dict__ for bsp in bsp_setpoint_list]})

                # log.debug(f"BSP activation done in: {round(time()-time_loop_start,3)} secounds.")
                # refresh rate

        sleep(REFRESH_RATE_CALC_BSP_SETPOINTS)
