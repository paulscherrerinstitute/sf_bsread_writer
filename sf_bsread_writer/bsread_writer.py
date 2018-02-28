

    def write_messages(self, start_pulse_id):
        self._logger.info("Writing channels to output_file '%s'.", self.output_file)

        try:
            first_iteration = True

            if start_pulse_id < self._buffer[0].data.pulse_id:
                self._logger.warning("start_pulse_id < oldest buffered message pulse_id")

            with h5py.File(self.output_file, 'w') as h5_file:
                while self._running_event.is_set():
                    if len(self._buffer) == 0:
                        sleep(0.1)  # wait for more messages being buffered
                        continue

                    # process the oldest buffered message
                    next_msg = self._buffer.popleft()
                    msg_pulse_id = next_msg.data.pulse_id

                    if self.end_pulse_id and self.end_pulse_id < msg_pulse_id:
                        # no more messages to write
                        end_pulse_id = self.end_pulse_id
                        self.end_pulse_id = None

                        # finilize hdf5 file
                        if end_pulse_id < msg_pulse_id:
                            self.prune_messages_in_hdf5(h5_file, end_pulse_id)

                        break

                    if msg_pulse_id < start_pulse_id:
                        self._logger.debug('Discard %d', msg_pulse_id)
                        continue  # discard the message

                    self._logger.debug('Write to hdf5 %d', msg_pulse_id)
                    self.write_message_to_hdf5(h5_file, next_msg, first_iteration)
                    first_iteration = False

        except:
            self._logger.exception("Error while writing bsread stream.")

    @staticmethod
    def write_message_to_hdf5(h5_file, message, first_iteration):
        """ Placeholder for a function to write message's content into hdf5 file.

        Intended for debugging. Saves only pulse_ids of messages!
        """
        if first_iteration:
            dset_pulse_id = h5_file.create_dataset('pulse_id', dtype='i8', shape=(1, 1), maxshape=(None, 1))
        else:
            dset_pulse_id = h5_file['pulse_id']
            dset_pulse_id.resize(dset_pulse_id.shape[0] + 1, axis=0)

        dset_pulse_id[-1] = message.data.pulse_id

    @staticmethod
    def prune_messages_in_hdf5(h5_file, end_pulse_id):
        """ Placeholder for a function to prune hdf5 file content down to end_pulse_id.

        Intended for debugging.
        """
        dset_pulse_id = h5_file['pulse_id']
        while dset_pulse_id[-1] > end_pulse_id:
            # this will also discard the data
            # see the Note at http://docs.h5py.org/en/latest/high/dataset.html#resizable-datasets
            dset_pulse_id.resize(dset_pulse_id.shape[0] - 1, axis=0)
