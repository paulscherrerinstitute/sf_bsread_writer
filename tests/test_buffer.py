import signal
import unittest
from collections import deque

from multiprocessing import Process, Event
from time import sleep

import os

from bsread.sender import sender, PUSH, PULL

from sf_bsread_writer import buffer


class TestBsreadBuffer(unittest.TestCase):
    def setUp(self):
        self.stream_port = 12345

        self.buffer = deque(maxlen=10)

        self.runningEvent = Event()
        self.runningEvent.set()

        self.buffer_process = Process(target=buffer.buffer_bsread_messages,
                                      args=("tcp://127.0.0.1:%d" % self.stream_port,
                                            self.buffer,
                                            self.runningEvent,
                                            True,
                                            1000,
                                            PULL))
        self.buffer_process.start()

        # Give it some time to start.
        sleep(1)

    def tearDown(self):
        self.buffer_process.terminate()
        sleep(0.5)

        try:
            os.kill(self.buffer_process.pid, signal.SIGINT)
        except:
            pass

        # Wait for the server to die.
        sleep(0.5)

    def test_classic_interaction(self):

        data = {}

        with sender(port=self.stream_port, mode=PUSH, queue_size=1) as output_stream:
            for index in range(10):
                pulse_id = index
                data["device1"] = index + 100
                data["device2"] = index + 200
                data["device3"] = None

                output_stream.send(pulse_id=pulse_id,
                                   data=data)

        self.runningEvent.clear()
        sleep(1.5)

        self.assertFalse(self.buffer_process.is_alive())
