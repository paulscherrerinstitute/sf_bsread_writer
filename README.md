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
5. [Manual test and function demo](#manual_test)

<a id="quick_start"></a>
## Quick start

The project is made up of 2 parts:

- Buffer service (buffer the incoming messages)
- Writer process (write the desired messages from the buffer)

Due to a relatively high start lag in getting bsread data, connecting to the source when the DAQ system is triggered 
does not work (you miss the first few messages). If, for example, the detector starts getting images with pulse_id 100,
bsread would start getting messages with pulse_id 120 (the actual gap varies based on the repetition rate). 
We need bsread messages from pulse_id 100 on to be stored together with the detector images, 
this is why we buffer all the messages all the time.

For info on how to run the buffer and writer, please see [Running the servers](#running_the_servers).

### Buffer service
The buffer service is running in the background. It accepts a tcp stream address - if you want to buffer a different stream,
you have to re-start the buffer. Using **systemd** for running the service is recommended.

The buffer sends out buffered messages to whoever it connects to its output port.

### Writer process
The writer process is lunched and stopped once per DAQ acquisition. It connects to the buffer service and starts 
writing messages to disk from the specified start_pulse_id until the specified stop_pulse_id. Alternativelly, you can 
tell the writer to write the messages without knowing the pulse_id - write from the moment you call start_now until 
the moment you call stop_now. Please note that this mode is not synchronized with any detector.

You specify the start and stop pulse_id by calling its REST Api (see [REST API](#rest_api)).

The writer uses the SwissFEL specific file format (the same as sf_databuffer_writer).

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

usage: sf_bsread_buffer [-h] [-o OUTPUT_PORT] [-b BUFFER_LENGTH]
                        [--log_level {CRITICAL,ERROR,WARNING,INFO,DEBUG}]
                        [--analyzer]

bsread buffer

positional arguments:
  stream        Stream source in format tcp://127.0.0.1:10000

optional arguments:
  -h, --help            show this help message and exit
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

All request (with the exception of **start\_pulse\_id**, **stop\_pulse\_id**, **start\_now**, **stop\_now** 
and **kill**) return a JSON with the following fields:
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
    
* `PUT localhost:8888/start_now` - start the acquisition and discard messages before the current timestamp.
    - Empty response.

* `PUT localhost:8888/stop_now` - stop the acquisition and discard messages after the current timestamp.
    - Empty response.
    
<a id="manual_test"></a>
## Manual test and function demo

This is just a procedure you can do to better understand the moving parts of this project. You start by first cloning 
the project and having the needed libraries in your Python path. The easiest is to do it on SF consoles, and loading 
our default Python environment.

You will need 3 consoles for this test, you can also do it in 1, but you will need to be a bit more flexible 
with the outputs.

Pick your preferred camera and get the stream address from it (BSREADCONFIG PV):

```bash
caget SLG-LCAM-C041:BSREADCONFIG
# SLG-LCAM-C041:BSREADCONFIG "tcp://daqsf-sioc-cs-01:8050"
```

You will receive the stream address on the ZMQ network - you probably do not have access to this from the consoles,
so for this test we will remove the "daq" from the "daqsf" part.


### First console: Buffer

From the root of the cloned project:

```bash
export PYTHONPATH=`pwd`

# Connect to stream discovered above, use port 12300 to stream images out, and the buffer should have 100 elements (4s)
python sf_bsread_writer/buffer.py tcp://sf-sioc-cs-01:8050 -o 12300 -b 100 --log_level DEBUG

# The expected output....
[INFO] Connecting to stream 'tcp://sf-sioc-cs-01:8050'.
[INFO] Requesting stream from: tcp://sf-sioc-cs-01:8050
[INFO] Input stream connecting to 'tcp://sf-sioc-cs-01:8050'.
[INFO] Input stream host 'sf-sioc-cs-01' and port '8050'.
[2019-07-02 18:00:06,028][mflow.mflow][INFO] Connected to tcp://sf-sioc-cs-01:8050
[INFO] Connected to tcp://sf-sioc-cs-01:8050
[2019-07-02 18:00:06,028][mflow.mflow][INFO] Receive timeout set: 1000.000000
[INFO] Receive timeout set: 1000.000000
[INFO] Output stream binding to port '12300'.
[2019-07-02 18:00:06,030][mflow.mflow][INFO] Bound to tcp://*:12300
[INFO] Started listening to the stream.
[INFO] Bound to tcp://*:12300
[DEBUG] Message with pulse_id 9066880403 and timestamp 1562083206.748058 added to the buffer.
[DEBUG] Sending message with pulse_id '9066880403'.
[DEBUG] Update channel metadata.
[DEBUG] Message with pulse_id 9066880503 and timestamp 1562083207.6237204 added to the buffer.
[DEBUG] Message with pulse_id 9066880603 and timestamp 1562083208.5736806 added to the buffer.
[DEBUG] Message with pulse_id 9066880703 and timestamp 1562083209.5877638 added to the buffer.
[DEBUG] Message with pulse_id 9066880803 and timestamp 1562083210.6012282 added to the buffer.
```

This started the buffer. Let it run - in a real world situation this would be started as a systemd service.

### Second console: Writer

A writer instance needs to be started for each file we want to write. It works the same way as the detector writer.

From the root of the cloned project:

```bash
export PYTHONPATH=`pwd`

# Start the writer, connect to 12300 to get images, write works.h5, 
# -1=="do not change current user", REST Api on port 4000 
python sf_bsread_writer/writer.py tcp://127.0.0.1:12300 works.h5 -1 4000 --log_level=DEBUG

# The expected output.....

[INFO] __main__ - Starting writer manager with stream_address tcp://127.0.0.1:12300, output_file works.h5.
[INFO] __main__ - Not changing process uid and gid.
[INFO] __main__ - Writing output file to folder ''.
[INFO] __main__ - Folder '' already exists.
[INFO] __main__ - Starting rest API on port 4000.
Bottle v0.12.13 server starting up (using WSGIRefServer())...
Listening on http://127.0.0.1:4000/
Hit Ctrl-C to quit.

# At the end.. Once you executed the REST calls from the third console you will also get something like this ...

127.0.0.1 - - [02/Jul/2019 18:35:54] "PUT /start_now HTTP/1.1" 200 0
[INFO] sf_bsread_writer.writer_format - Initializing format datasets.
[INFO] sf_bsread_writer.writer_format - Data header change detected.
[INFO] sf_bsread_writer.writer_rest - Stopping writing without pulse_id.
[INFO] __main__ - Set stop_pulse_id=None
127.0.0.1 - - [02/Jul/2019 18:36:01] "PUT /stop_now HTTP/1.1" 200 0
[INFO] sf_bsread_writer.writer_format - Starting to close the file.
[INFO] bsread.writer - Compact data for dataset /data/SLG-LCAM-C041:FPICTURE/data from 1001 to 6
[INFO] bsread.writer - Compact data for dataset /data/SLG-LCAM-C041:FPICTURE/pulse_id from 1001 to 6
[INFO] bsread.writer - Compact data for dataset /data/SLG-LCAM-C041:FPICTURE/is_data_present from 1001 to 6
[INFO] bsread.writer - Close file /
[INFO] sf_bsread_writer.writer_format - File closed in 0.23509478569030762 seconds.
[INFO] __main__ - Stopping bsread writer at pulse_id: None
[2019-07-02 18:36:02,032][mflow.mflow][INFO] Disconnected
[INFO] mflow.mflow - Disconnected
[INFO] __main__ - Writing completed. Timestamp range from 1562085354.8176036 to 1562085361.4905198 written to file.

```

### Third console: Rest calls

This is the most simple example, without knowing the pulse_id but by using the current timestamp.

```bash
curl -X PUT localhost:4000/start_now

# Wait a couple of seconds.

curl -X PUT localhost:4000/stop_now

# You should find the file **works.h5** in the root folder of the git repo.
ll
# ...
total 16M
drwxr-xr-x 2 babic_a 2.0K Jul  2 17:41 conda-recipe
-rw-r--r-- 1 babic_a 7.0K Jul  2 17:41 README.md
-rw-r--r-- 1 babic_a  350 Jul  2 17:41 setup.py
drwxr-xr-x 3 babic_a 2.0K Jul  2 18:35 sf_bsread_writer
drwxr-xr-x 2 babic_a 2.0K Jul  2 17:41 tests
-rw-r--r-- 1 babic_a  16M Jul  2 18:36 works.h5
[sf-lc6a-64-06 sf_bsread_writer]$

# Check if you image is in the file.
h5ls works.h5/data/SLG-LCAM-C041:FPICTURE
# ...
data                     Dataset {6/Inf, 1024, 1280}
is_data_present          Dataset {6/Inf}
pulse_id                 Dataset {6/Inf}

```

Once you call **stop** on the writer, the file will be closed and the process will terminate.