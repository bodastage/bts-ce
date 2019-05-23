[![Join the chat at https://gitter.im/bodastage/bts-ce](https://badges.gitter.im/bodastage/bts-ce.svg)](https://gitter.im/bodastage/bts-ce?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Github All Releases](https://img.shields.io/github/downloads/bodastage/bts-ce/total.svg)](https://github.com/bodastage/bts-ce/releases/latest) [![GitHub repo size in bytes](https://img.shields.io/github/repo-size/bodastage/bts-ce.svg)](https://github.com/bodastage/bts-ce) [![Read the Docs](https://img.shields.io/readthedocs/bts-ce-docs.svg)]() [![GitHub release](https://img.shields.io/github/release/bodastage/bts-ce.svg)](https://github.com/bodastage/bts-ce/releases) [![license](https://img.shields.io/github/license/bodastage/bts-ce.svg)](https://raw.githubusercontent.com/bodastage/bts-ce/master/LICENCE)

![BTS-CE Logo](/images/btsce-logo-named-selection.png)

## Boda Telecom Suite Community Edition (BTS-CE)

Boda Telecom Suite Community Edition - An open source telecommunication network management platform

## Features

* Generation of network topology from configuration data
* Radio network element browsing
* CM managed object browsing 
* Automatic network baseline generation
* Radio Access Network (RAN) audit (relations, conflicts, parameter values vs baseline )
* Reports that support tabular and different graphical presentation of data

## Requirements 

* 64 bit OS (Kernel version 3.10+ for Linux and build 10.0.14393+ for Windows)
* [Docker](https://www.docker.com/get-docker)
* Memery and disk space depend on the network /data size
* Latest web browser

## Deployment/Installation

* Download latest release files (bts-ce-**version**) from https://github.com/bodastage/bts-ce/releases/latest
* Unzip the downloaded files
* Launch the **command prompt**
* From the commad prompt, change to unzipped BTS-CE folder
  ```batch 
  > cd  /path/to/bts-ce-<version>
  ```
* Run setup command and follow any instructions given
  ```batch 
  > bts setup
  ```
* Open web broswer and paste the URL http://localhost:8888
* Login with username: **btsuser@bodastage.org** and password: **password**

## Installing a new release 
At the moment, we don't have upgrade scripts. You have to remove the previous version and install the new one. 
This is achieved using the following 2 commands,

* From the old version's directory, run 
	```batch 
	bts rm
	```
* From the new version's directory, run 
	```batch 
	bts create
	```

## Supported Web Browsers

| Desktop Browsers | Version |
| -------- | -------- |
| Safari | 6.1+ |
| Google Chrome | 32+ |
| Microsoft Edge | |
| Firefox | 27+ |

## Built With
- [Python](https://www.python.org)
- [PostgreSQL](https://www.postgresql.org/)
- [Apache Airflow](https://airflow.apache.org/)
- [ReactJs](http://reactjs.org/)
- [RabbitMQ](https://www.rabbitmq.com/)

## Resources

* [Online Documentation](http://docs.bodastage.com)

## Copyright / License

Copyright 2017 - 2019 [Bodastage Solutions](http://www.bodastage.com)

Licensed under the Apache License, Version 2.0 ; you may not use this work except in compliance with the License. You may obtain a copy of the License in the LICENSE file, or at:

https://www.apache.org/licenses/LICENSE-2.0

