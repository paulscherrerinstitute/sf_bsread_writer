import logging
from logging import getLogger

from bsread import dispatcher, SUB, Source
from bsread.handlers import extended

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')

_logger = getLogger(__name__)

n_connections = 10
n_messages_per_connection = 50

_logger.info("Doing %d connection, each with %d messages.", n_connections, n_messages_per_connection)

channels = [
    "SAROP21-CVME-PBPS2:Lnk9Ch6-BG-DATA",
    "SAROP21-CVME-PBPS2:Lnk9Ch6-BG-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS2:Lnk9Ch6-BG-DATA-SUM",
    "SAROP21-CVME-PBPS2:Lnk9Ch6-BG-DRS_TC",
    "SAROP21-CVME-PBPS2:Lnk9Ch6-BG-PULSEID-valid",
    "SAROP21-CVME-PBPS2:Lnk9Ch6-DATA",
    "SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-SUM",
    "SAROP21-CVME-PBPS2:Lnk9Ch6-DRS_TC",
    "SAROP21-CVME-PBPS2:Lnk9Ch6-PULSEID-valid",
    "SAROP21-CVME-PBPS2:Lnk9Ch6-WD-gain-RBa",
    "SAROP21-CVME-PBPS2:Lnk9Ch7-BG-DATA",
    "SAROP21-CVME-PBPS2:Lnk9Ch7-BG-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS2:Lnk9Ch7-BG-DATA-SUM",
    "SAROP21-CVME-PBPS2:Lnk9Ch7-BG-DRS_TC",
    "SAROP21-CVME-PBPS2:Lnk9Ch7-BG-PULSEID-valid",
    "SAROP21-CVME-PBPS2:Lnk9Ch7-DATA",
    "SAROP21-CVME-PBPS2:Lnk9Ch7-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS2:Lnk9Ch7-DATA-SUM",
    "SAROP21-CVME-PBPS2:Lnk9Ch7-DRS_TC",
    "SAROP21-CVME-PBPS2:Lnk9Ch7-PULSEID-valid",
    "SAROP21-CVME-PBPS2:Lnk9Ch7-WD-gain-RBa",
    "SAROP21-CVME-PBPS1:Lnk9Ch12-BG-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS1:Lnk9Ch12-BG-DATA-SUM",
    "SAROP21-CVME-PBPS1:Lnk9Ch12-BG-DRS_TC",
    "SAROP21-CVME-PBPS1:Lnk9Ch12-BG-PULSEID-valid",
    "SAROP21-CVME-PBPS1:Lnk9Ch12-DATA",
    "SAROP21-CVME-PBPS1:Lnk9Ch12-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS1:Lnk9Ch12-DATA-SUM",
    "SAROP21-CVME-PBPS1:Lnk9Ch12-DRS_TC",
    "SAROP21-CVME-PBPS1:Lnk9Ch12-PULSEID-valid",
    "SAROP21-CVME-PBPS1:Lnk9Ch12-WD-gain-RBa",
    "SAROP21-CVME-PBPS1:Lnk9Ch13-BG-DATA",
    "SAROP21-CVME-PBPS1:Lnk9Ch13-BG-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS1:Lnk9Ch13-BG-DATA-SUM",
    "SAROP21-CVME-PBPS1:Lnk9Ch13-BG-DRS_TC",
    "SAROP21-CVME-PBPS1:Lnk9Ch13-BG-PULSEID-valid",
    "SAROP21-CVME-PBPS1:Lnk9Ch13-DATA",
    "SAROP21-CVME-PBPS1:Lnk9Ch13-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS1:Lnk9Ch13-DATA-SUM",
    "SAROP21-CVME-PBPS1:Lnk9Ch13-DRS_TC",
    "SAROP21-CVME-PBPS1:Lnk9Ch13-PULSEID-valid",
    "SAROP21-CVME-PBPS1:Lnk9Ch13-WD-gain-RBa",
    "SAROP21-CVME-PBPS1:Lnk9Ch14-BG-DATA",
    "SAROP21-CVME-PBPS1:Lnk9Ch14-BG-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS1:Lnk9Ch14-BG-DATA-SUM",
    "SAROP21-CVME-PBPS1:Lnk9Ch14-BG-DRS_TC",
    "SAROP21-CVME-PBPS1:Lnk9Ch14-BG-PULSEID-valid",
    "SAROP21-CVME-PBPS1:Lnk9Ch14-DATA",
    "SAROP21-CVME-PBPS1:Lnk9Ch14-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS1:Lnk9Ch14-DATA-SUM",
    "SAROP21-CVME-PBPS1:Lnk9Ch14-DRS_TC",
    "SAROP21-CVME-PBPS1:Lnk9Ch14-PULSEID-valid",
    "SAROP21-CVME-PBPS1:Lnk9Ch14-WD-gain-RBa",
    "SAROP21-CVME-PBPS1:Lnk9Ch15-BG-DATA",
    "SAROP21-CVME-PBPS1:Lnk9Ch15-BG-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS1:Lnk9Ch15-BG-DATA-SUM",
    "SAROP21-CVME-PBPS1:Lnk9Ch15-BG-DRS_TC",
    "SAROP21-CVME-PBPS1:Lnk9Ch15-BG-PULSEID-valid",
    "SAROP21-CVME-PBPS1:Lnk9Ch15-DATA",
    "SAROP21-CVME-PBPS1:Lnk9Ch15-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS1:Lnk9Ch15-DATA-SUM",
    "SAROP21-CVME-PBPS1:Lnk9Ch15-DRS_TC",
    "SAROP21-CVME-PBPS1:Lnk9Ch15-PULSEID-valid",
    "SAROP21-CVME-PBPS1:Lnk9Ch15-WD-gain-RBa",
    "SAROP21-CVME-PBPS1:Lnk9Ch15-WD_FREQ",
    "SAROP21-CVME-PBPS2:Lnk9Ch12-BG-DATA-SUM",
    "SAROP21-CVME-PBPS2:Lnk9Ch12-BG-DRS_TC",
    "SAROP21-CVME-PBPS2:Lnk9Ch12-BG-PULSEID-valid",
    "SAROP21-CVME-PBPS2:Lnk9Ch12-DATA",
    "SAROP21-CVME-PBPS2:Lnk9Ch12-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS2:Lnk9Ch12-DATA-SUM",
    "SAROP21-CVME-PBPS2:Lnk9Ch12-DRS_TC",
    "SAROP21-CVME-PBPS2:Lnk9Ch12-PULSEID-valid",
    "SAROP21-CVME-PBPS2:Lnk9Ch12-WD-gain-RBa",
    "SAROP21-CVME-PBPS2:Lnk9Ch13-BG-DATA",
    "SAROP21-CVME-PBPS2:Lnk9Ch13-BG-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS2:Lnk9Ch13-BG-DATA-SUM",
    "SAROP21-CVME-PBPS2:Lnk9Ch13-BG-DRS_TC",
    "SAROP21-CVME-PBPS2:Lnk9Ch13-BG-PULSEID-valid",
    "SAROP21-CVME-PBPS2:Lnk9Ch13-DATA",
    "SAROP21-CVME-PBPS2:Lnk9Ch13-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS2:Lnk9Ch13-DATA-SUM",
    "SAROP21-CVME-PBPS2:Lnk9Ch13-DRS_TC",
    "SAROP21-CVME-PBPS2:Lnk9Ch13-PULSEID-valid",
    "SAROP21-CVME-PBPS2:Lnk9Ch13-WD-gain-RBa",
    "SAROP21-CVME-PBPS2:Lnk9Ch14-BG-DATA",
    "SAROP21-CVME-PBPS2:Lnk9Ch14-BG-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS2:Lnk9Ch14-BG-DATA-SUM",
    "SAROP21-CVME-PBPS2:Lnk9Ch14-BG-DRS_TC",
    "SAROP21-CVME-PBPS2:Lnk9Ch14-BG-PULSEID-valid",
    "SAROP21-CVME-PBPS2:Lnk9Ch14-DATA",
    "SAROP21-CVME-PBPS2:Lnk9Ch14-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS2:Lnk9Ch14-DATA-SUM",
    "SAROP21-CVME-PBPS2:Lnk9Ch14-DRS_TC",
    "SAROP21-CVME-PBPS2:Lnk9Ch14-PULSEID-valid",
    "SAROP21-CVME-PBPS2:Lnk9Ch14-WD-gain-RBa",
    "SAROP21-CVME-PBPS2:Lnk9Ch15-BG-DATA",
    "SAROP21-CVME-PBPS2:Lnk9Ch15-BG-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS2:Lnk9Ch15-BG-DATA-SUM",
    "SAROP21-CVME-PBPS2:Lnk9Ch15-BG-DRS_TC",
    "SAROP21-CVME-PBPS2:Lnk9Ch15-BG-PULSEID-valid",
    "SAROP21-CVME-PBPS2:Lnk9Ch15-DATA",
    "SAROP21-CVME-PBPS2:Lnk9Ch15-DATA-CALIBRATED",
    "SAROP21-CVME-PBPS2:Lnk9Ch15-DATA-SUM",
    "SAROP21-CVME-PBPS2:Lnk9Ch15-DRS_TC",
    "SAROP21-CVME-PBPS2:Lnk9Ch15-PULSEID-valid",
    "SAROP21-CVME-PBPS2:Lnk9Ch15-WD-gain-RBa",
    "SAROP21-CVME-PBPS2:Lnk9Ch15-WD_FREQ",
    "SARFE10-CVME-PHO6211:Lnk9Ch12-BG-DATA",
    "SARFE10-CVME-PHO6211:Lnk9Ch12-BG-DATA-CALIBRATED",
    "SARFE10-CVME-PHO6211:Lnk9Ch12-BG-DRS_TC",
    "SARFE10-CVME-PHO6211:Lnk9Ch12-DATA",
    "SARFE10-CVME-PHO6211:Lnk9Ch12-DATA-CALIBRATED",
    "SARFE10-CVME-PHO6211:Lnk9Ch12-DATA-SUM",
    "SARFE10-CVME-PHO6211:Lnk9Ch12-DRS_TC",
    "SARFE10-CVME-PHO6211:Lnk9Ch13-BG-DATA",
    "SARFE10-CVME-PHO6211:Lnk9Ch13-BG-DATA-CALIBRATED",
    "SARFE10-CVME-PHO6211:Lnk9Ch13-BG-DRS_TC",
    "SARFE10-CVME-PHO6211:Lnk9Ch13-DATA",
    "SARFE10-CVME-PHO6211:Lnk9Ch13-DATA-CALIBRATED",
    "SARFE10-CVME-PHO6211:Lnk9Ch13-DATA-SUM",
    "SARFE10-CVME-PHO6211:Lnk9Ch13-DRS_TC",
    "SARFE10-CVME-PHO6211:Lnk9Ch14-BG-DATA",
    "SARFE10-CVME-PHO6211:Lnk9Ch14-BG-DATA-CALIBRATED",
    "SARFE10-CVME-PHO6211:Lnk9Ch14-BG-DRS_TC",
    "SARFE10-CVME-PHO6211:Lnk9Ch14-DATA",
    "SARFE10-CVME-PHO6211:Lnk9Ch14-DATA-CALIBRATED",
    "SARFE10-CVME-PHO6211:Lnk9Ch14-DATA-SUM",
    "SARFE10-CVME-PHO6211:Lnk9Ch14-DRS_TC",
    "SARFE10-CVME-PHO6211:Lnk9Ch15-BG-DATA",
    "SARFE10-CVME-PHO6211:Lnk9Ch15-BG-DATA-CALIBRATED",
    "SARFE10-CVME-PHO6211:Lnk9Ch15-BG-DRS_TC",
    "SARFE10-CVME-PHO6211:Lnk9Ch15-DATA",
    "SARFE10-CVME-PHO6211:Lnk9Ch15-DATA-CALIBRATED",
    "SARFE10-CVME-PHO6211:Lnk9Ch15-DATA-SUM",
    "SARFE10-CVME-PHO6211:Lnk9Ch15-DRS_TC",
    "SARFE10-PBPG050:HAMP-INTENSITY",
    "SARFE10-PBPG050:HAMP-INTENSITY-AVG",
    "SARFE10-PBPG050:HAMP-INTENSITY-CAL",
    "SARFE10-PBPG050:HAMP-XPOS",
    "SARFE10-PBPG050:HAMP-YPOS",
    "SARCL02-DBPM110:X1-VALID",
    "SARCL02-DBPM220:X1",
    "SARCL02-DBPM220:X1-VALID",
    "SARCL02-DBPM260:X1",
    "SARCL02-DBPM260:X1-VALID",
    "SARCL02-DBPM330:X1",
    "SARCL02-DBPM330:X1-VALID",
    "SARCL02-DBPM470:X1",
    "SARCL02-DBPM470:X1-VALID",
    "SARBD02-DBPM040:Y1",
    "SARBD02-DBPM040:Y1-VALID",
    "S10BC01-DBPM010:T1",
    "S10BC01-DBPM050:T1",
    "S10BC01-DBPM090:T1",
    "S10BC02-DBPM140:T1",
    "S10BC02-DBPM320:T1",
    "S10BD01-DBPM020:T1",
    "S10CB01-DBPM220:T1",
    "S10CB01-DBPM420:T1",
    "S10CB02-DBPM220:T1",
    "S10CB02-DBPM420:T1",
    "S10CB03-DBPM220:T1",
    "S10CB03-DBPM420:T1",
    "S10CB04-DBPM220:T1",
    "S10CB04-DBPM420:T1",
    "S10CB05-DBPM220:T1",
    "S10CB05-DBPM420:T1",
    "S10CB06-DBPM220:T1",
    "S10CB06-DBPM420:T1",
    "S10CB07-DBPM220:T1",
    "S10CB07-DBPM420:T1",
    "S10CB08-DBPM220:T1",
    "S10CB08-DBPM420:T1",
    "S10CB09-DBPM220:T1",
    "S10DI01-DBPM110:T1",
    "S10MA01-DBPM010:T1",
    "S10MA01-DBPM060:T1",
    "S10MA01-DBPM120:T1",
    "S20CB01-DBPM420:T1",
    "S20CB02-DBPM420:T1",
    "S20CB03-DBPM420:T1",
    "S20SY01-DBPM010:T1",
    "S20SY01-DBPM040:T1",
    "S20SY01-DBPM060:T1",
    "S20SY02-DBPM080:T1",
    "S20SY02-DBPM120:T1",
    "S20SY02-DBPM150:T1",
    "S20SY03-DBPM010:T1",
    "S20SY03-DBPM040:T1",
    "S20SY03-DBPM080:T1",
    "S30CB01-DBPM420:T1",
    "S30CB02-DBPM420:T1",
    "S30CB03-DBPM420:T1",
    "S30CB04-DBPM420:T1",
    "S30CB05-DBPM420:T1",
    "S30CB06-DBPM420:T1",
    "S30CB07-DBPM420:T1",
    "S30CB08-DBPM420:T1",
    "S30CB09-DBPM420:T1",
    "S30CB10-DBPM420:T1",
    "S30CB11-DBPM420:T1",
    "S30CB12-DBPM420:T1",
    "S30CB13-DBPM420:T1",
    "S30CB14-DBPM420:T1",
    "S30CB15-DBPM420:T1",
    "SARBD01-DBPM040:T1",
    "SARBD02-DBPM010:T1",
    "SARBD02-DBPM040:T1",
    "SARCL01-DBPM010:T1",
    "SARCL01-DBPM060:T1",
    "SARCL01-DBPM120:T1",
    "SARCL01-DBPM150:T1",
    "SARCL02-DBPM110:T1",
    "SARCL02-DBPM220:T1",
    "SARCL02-DBPM260:T1",
    "SARCL02-DBPM330:T1",
    "SARCL02-DBPM470:T1",
    "SARMA01-DBPM040:T1",
    "SARMA01-DBPM100:T1",
    "SARMA02-DBPM010:T1",
    "SARMA02-DBPM020:T1",
    "SARMA02-DBPM040:T1",
    "SARMA02-DBPM110:T1",
    "SARUN01-DBPM070:T1",
    "SARUN02-DBPM070:T1",
    "SARUN03-DBPM070:T1",
    "SARUN04-DBPM070:T1",
    "SARUN05-DBPM070:T1",
    "SARUN06-DBPM070:T1",
    "SARUN07-DBPM070:T1",
    "SARUN08-DBPM070:T1",
    "SARUN09-DBPM070:T1",
    "SARUN10-DBPM070:T1",
    "SARUN11-DBPM070:T1",
    "SARUN12-DBPM070:T1",
    "SARUN13-DBPM070:T1",
    "SARUN14-DBPM070:T1",
    "SARUN15-DBPM070:T1",
    "SARUN16-DBPM070:T1",
    "SARUN17-DBPM070:T1",
    "SARUN18-DBPM070:T1",
    "SARUN19-DBPM070:T1",
    "SARUN20-DBPM070:T1",
    "SINBC01-DBPM010:T1",
    "SINBC01-DBPM030:T1",
    "SINBC01-DBPM080:T1",
    "SINBC01-DBPM100:T1",
    "SINBC02-DBPM140:T1",
    "SINBC02-DBPM320:T1",
    "SINDI01-DBPM010:T1",
    "SINDI01-DBPM060:T1",
    "SINDI02-DBPM010:T1",
    "SINDI02-DBPM040:T1",
    "SINDI02-DBPM080:T1",
    "SINEG01-DBPM340:T1",
    "SINLH01-DBPM060:T1",
    "SINLH02-DBPM210:T1",
    "SINLH02-DBPM240:T1",
    "SINLH03-DBPM010:T1",
    "SINLH03-DBPM050:T1",
    "SINLH03-DBPM090:T1",
    "SINSB01-DBPM150:T1",
    "SINSB02-DBPM150:T1",
    "SINSB03-DBPM120:T1",
    "SINSB03-DBPM220:T1",
    "SINSB04-DBPM120:T1",
    "SINSB04-DBPM220:T1",
    "SINSB05-DBPM120:T1",
    "SINSB05-DBPM220:T1",
    "SINXB01-DBPM120:T1",
    "S10BC01-DBAM070:BAM_CH1_NORM",
    "S10BC01-DBAM070:BAM_CH2_NORM",
    "S10BC01-DBAM070:EOM1_RESOLUTION1",
    "S10BC01-DBAM070:EOM1_T1",
    "S10BC01-DBAM070:EOM1_T1-VALID",
    "S10BC01-DBAM070:EOM1_T2",
    "S10BC01-DBAM070:EOM1_T2-VALID",
    "S10BC01-DBAM070:EOM2_T1",
    "S10BC01-DBAM070:EOM2_T1-VALID",
    "S10BC01-DBAM070:EOM2_T2",
    "S10BC01-DBAM070:EOM2_T2-VALID",
    "S10BC01-DBAM070:GPAC_EOM1_arrt_B1",
    "S10BC01-DBAM070:GPAC_EOM1_arrt_B2",
    "S10BC01-DBAM070:GPAC_EOM2_arrt_B1",
    "S10BC01-DBAM070:GPAC_EOM2_arrt_B2",
    "S10BC01-DBAM070:MX80_ENC_T",
    "SINLH01-DBAM010:BAM_CH1_NORM",
    "SINLH01-DBAM010:BAM_CH2_NORM",
    "SINLH01-DBAM010:EOM1_RESOLUTION1",
    "SINLH01-DBAM010:EOM1_T1",
    "SINLH01-DBAM010:EOM1_T1-VALID",
    "SINLH01-DBAM010:EOM1_T2",
    "SINLH01-DBAM010:EOM1_T2-VALID",
    "SINLH01-DBAM010:EOM2_T1",
    "SINLH01-DBAM010:EOM2_T1-VALID",
    "SINLH01-DBAM010:EOM2_T2",
    "SINLH01-DBAM010:EOM2_T2-VALID",
    "SINLH01-DBAM010:GPAC_EOM1_arrt_B1",
    "SINLH01-DBAM010:GPAC_EOM1_arrt_B2",
    "SINLH01-DBAM010:GPAC_EOM2_arrt_B1",
    "SINLH01-DBAM010:GPAC_EOM2_arrt_B2",
    "SINLH01-DBAM010:MX80_ENC_T"
]

_logger.info("Requesting channels: %s", channels)

handler = extended.Handler()

channels_definitions = {}

for n_connection in range(n_connections):
    _logger.info("Starting connection number %d." % n_connection)

    stream_address = dispatcher.request_stream(channels)
    source_host, source_port = stream_address.rsplit(":", maxsplit=1)

    source_host = source_host.split("//")[1]
    source_port = int(source_port)

    _logger.info("Input stream host '%s' and port '%s'.", source_host, source_port)

    stream = Source(host=source_host, port=source_port, mode=SUB, receive_timeout=1000)
    stream.connect()

    n_received_messages = 0

    while n_received_messages < n_messages_per_connection:
        message = stream.receive(handler=handler.receive)

        # In case you set a receive timeout, the returned message can be None.
        if message is None:
            _logger.debug("Empty message.")
            continue

        n_received_messages += 1

        pulse_id = message.data["header"]["pulse_id"]

        _logger.debug("Received message with pulse_id '%s'.", pulse_id)

        message_channels = []

        for index, channel in enumerate(message.data["data_header"]["channels"]):
            name = channel["name"]
            dtype = channel.get("type", "float64")
            shape = channel.get("shape", [1])
            encoding = channel.get("encoding", "<")

            message_channels.append(name)

            message_channel_definition = {"type": dtype,
                                          "shape": shape,
                                          "encoding": encoding}

            if name not in channels_definitions:
                _logger.info("Channel '%s' seen for the first time: type=%s, shape=%s, encoding=%s",
                             name, dtype, shape, encoding)

                channels_definitions[name] = message_channel_definition

            # Check if the channel definition has not changed.
            if channels_definitions[name] != message_channel_definition:
                _logger.error("Previous channel '%s' definition %s does not match the current message one %s.",
                              name, channels_definitions[name], message_channel_definition)

                channels_definitions[name] = message_channel_definition
                _logger.warning("Channel '%s' definition updated: type=%s, shape=%s, encoding=%s",
                                name, dtype, shape, encoding)

        # Check if all the requested channels are present in each message.
        if sorted(channels) != sorted(message_channels):
            _logger.error("Requested channels and received channels are not the same.\n"
                          "Requested: %s\n",
                          "Received: %s", sorted(channels), sorted(message_channels))

    stream.disconnect()
