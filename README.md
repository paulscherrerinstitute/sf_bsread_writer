[![Build Status](https://travis-ci.org/paulscherrerinstitute/sf_bsread_writer.svg?branch=master)](https://travis-ci.org/paulscherrerinstitute/sf_bsread_writer/)

# SwissFEL Bsread Writer
Buffering and writing solution for bsread data in SwissFEL DAQ system.

# Table of content
1. [Quick start](#quick_start)
2. [Build](#build)
    1. [Conda setup](#conda_setup)
    2. [Local build](#local_build)
3. [Running the servers](#running_the_servers)
4. [Web interface](#web_interface)
    1. [REST API](#rest_api)

<a id="quick_start"></a>
## Quick start

The project is made up of 2 parts:

- Buffer service (buffer the incoming messages)
- Writer process (write the desired messages from the buffer)

Due to a relatively high start lag in getting bsread data, connecting to the sources when the DAQ system is triggered 
does not work (you miss the first few messages). If, for example, the detector starts getting images with pulse_id 100,
bsread would start getting messages with pulse_id 120 (the actual gap varies based on the repetition rate). 
We need bsread messages from pulse_id 100 on to be stored together with the detector images, 
this is why we buffer all the messages all the time.

For info on how to run the buffer and writer, please see [Running the servers](#running_the_servers).

### Buffer service
The buffer service is running in the background. It accepts a list of channels to buffer - if you want to change the 
list of channels, you have to modify the config file, and restart the buffer. Using **systemd** for running the 
service is recommended.

The buffer sends out buffered messages to whoever it connects to its output port.

### Writer process
The writer process is lunched and stopped once per DAQ acquisition. It connects to the buffer service and starts 
writing messages to disk from the specified start_pulse_id until the specified stop_pulse_id.
You specify this 2 parameters by calling its REST Api (see [REST API](#rest_api)).

The writer uses the SwissFEL specific file format.

<a id="build"></a>
## Build

<a id="conda_setup"></a>
### Conda setup
If you use conda, you can create an environment with the sf_bsread_writer library by running:

```bash
conda create -c paulscherrerinstitute --name <env_name> sf_bsread_writer
```

After that you can just source you newly created environment and start using the server.

<a id="local_build"></a>
### Local build
You can build the library by running the setup script in the root folder of the project:

```bash
python setup.py install
```

or by using the conda also from the root folder of the project:

```bash
conda build conda-recipe
conda install --use-local sf_bsread_writer
```

#### Requirements
The library relies on the following packages:

- bsread >=1.1.0
- bottle
- requests

In case you are using conda to install the packages, you might need to add the **paulscherrerinstitute** channel to
your conda config:

```
conda config --add channels paulscherrerinstitute
```

<a id="running_the_servers"></a>
## Running the servers
There are 2 components to this repository:

- Buffer
- Writer

The buffer is meant to be a service that is permanently running, while the writer is started every time you want to 
write some messages to disk.

### Buffer

Start the buffer in the background, preferably as a systemd service. The buffer will shutdown every time there is an 
error it cannot recover from - it is best to have it on auto restart.

```bash
sf_bsread_buffer -h

usage: sf_bsread_buffer [-h] [-c CHANNELS_FILE] [-o OUTPUT_PORT] [-b BUFFER_LENGTH]
                        [--log_level {CRITICAL,ERROR,WARNING,INFO,DEBUG}]
                        [--analyzer]

bsread buffer

optional arguments:
  -h, --help            show this help message and exit
  -c CHANNELS_FILE, --channels_file CHANNELS_FILE
                        JSON file with channels to buffer.
  -o OUTPUT_PORT, --output_port OUTPUT_PORT
                        Port to bind the output stream to.
  -b BUFFER_LENGTH, --buffer_length BUFFER_LENGTH
                        Length of the ring buffer.
  --log_level {CRITICAL,ERROR,WARNING,INFO,DEBUG}
                        Log level to use.
  --analyzer            Analyze the incoming stream for anomalies.
```

### Writer

Start the writer every time you want some data to be collected from the buffer and written to disk. This step is 
usually part of the DAQ system.

It is important to note that the writer process is a "single usage" process - you start it, it writes down what you 
requested, and then it shuts down. You have to start the process for each acquisition you want to make.

```bash
sf_bsread_writer -h
usage: sf_bsread_writer [-h] [--log_level {CRITICAL,ERROR,WARNING,INFO,DEBUG}]
                        stream_address output_file user_id rest_port

bsread writer

positional arguments:
  stream_address        Address of the stream to connect to.
  output_file           File where to write the bsread stream.
  user_id               user_id under which to run the writer process.Use -1
                        for current user.
  rest_port             Port for REST api.

optional arguments:
  -h, --help            show this help message and exit
  --log_level {CRITICAL,ERROR,WARNING,INFO,DEBUG}
                        Log level to use.
```

<a id="web_interface"></a>
## Web interface

**WARNING**: Only the writer has a web interface - the buffer is just a service 
running in the background with no interaction.

All request (with the exception of **start\_pulse\_id**, **stop\_pulse\_id**, and **kill**) return a JSON 
with the following fields:
- **state** - \["ok", "error"\]
- **status** - What happened on the server or error message, depending on the state.
- Optional request specific field - \["statistics", "parameters"]

<a id="rest_api"></a>
### REST API
In the API description, localhost and port 8888 are assumed. Please change this for your specific case.

* `GET localhost:8888/status` - get the status of the writer.

* `POST localhost:8888/parameters` - set parameters of the writer.
    - Response specific field: "parameters" - Parameters you just set.  

* `GET localhost:8888/stop` - stop the writer.

* `GET localhost:8888/kill` - kill the writer process.
    - Empty response.

* `GET localhost:8888/statistics` - get writer process statistics.
    - Response specific field: "statistics" - Data about the writer.

* `PUT localhost:8888/start_pulse_id/<pulse_id>` - set first pulse_id to write to the output file.
    - Empty response.

* `PUT localhost:8888/stop_pulse_id/<pulse_id>` - set last pulse_id to write to the output file.
    - Empty response.