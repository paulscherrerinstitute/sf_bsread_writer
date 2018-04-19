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

<a id="web_interface"></a>
## Web interface

All request (with the exception of **start\_pulse\_id**, **stop\_pulse\_id**, and **kill**) return a JSON 
with the following fields:
- **state** - \["ok", "error"\]
- **status** - What happened on the server or error message, depending on the state.
- Optional request specific field - \["statistics", "parameters"]

<a id="rest_api"></a>
### REST API
