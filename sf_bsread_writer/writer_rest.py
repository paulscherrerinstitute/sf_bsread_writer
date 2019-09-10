import json
import logging
import os

import bottle

_logger = logging.getLogger(__name__)


def register_rest_interface(app, manager):
    @app.get("/status")
    def get_status():
        return {"state": "ok",
                "status": manager.get_status()}

    @app.post("/parameters")
    def set_parameters():
        manager.set_parameters(bottle.request.json)

        return {"state": "ok",
                "status": manager.get_status(),
                "parameters": manager.get_parameters()}

    @app.get("/stop")
    def stop():
        manager.stop()

        return {"state": "ok",
                "status": manager.get_status()}

    @app.get("/kill")
    def kill():
        os._exit(0)

    @app.get("/statistics")
    def get_statistics():
        return {"state": "ok",
                "status": manager.get_status(),
                "statistics": manager.get_statistics()}

    @app.put("/start_now")
    def start_now():
        _logger.info("Starting writer without pulse_id.")

        data = bottle.request.json()
        output_file = data.get("output_file")

        manager.start_writer(None, output_file)

    @app.put("/stop_now")
    def stop_now():
        _logger.info("Stopping writing without pulse_id.")

        manager.stop_writer(None)

    @app.put("/start_pulse_id/<pulse_id>")
    def start_pulse_id(pulse_id):
        _logger.info("Received start_pulse_id %s.", pulse_id)

        manager.start_writer(int(pulse_id))

    @app.put("/stop_pulse_id/<pulse_id>")
    def stop_pulse_id(pulse_id):
        _logger.info("Received stop_pulse_id %s.", pulse_id)

        manager.stop_writer(int(pulse_id))

    @app.error(500)
    def error_handler_500(error):
        bottle.response.content_type = 'application/json'
        bottle.response.status = 200

        error_text = str(error.exception)

        _logger.error(error_text)

        return json.dumps({"state": "error",
                           "status": error_text})
