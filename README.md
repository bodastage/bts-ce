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


## Requirements 

* 4GB memory
* 64 bit OS
* [Docker](https://www.docker.com/get-docker)
* 5GB hard disk space
* Latest web browser

## Deployment/Installation

### Windows
* Download latest release files from https://github.com/bodastage/bts-ce/releases/download/1.0.22-alpha-2/bts-ce-1.0.22-alpha-2.zip
* Unzip the downloaded files
* Launch the Windows **command prompt**
* From the commad prompt, change to unzipped BTS-CE folder
  ```batch 
  > cd  /path/to/bts-ce-<version>
  ```
* Run setup command and follow any instructions given
  ```batch 
  > bts setup
  ```
* Open web broswer and paste the URL http://localhost on windows or http://192.168.99.100
* Login with username: **btsuser@bodastage.org** and password: **password**

> If you are on a metered internet connection, note that the setup process will download about 1GB.

### Unix
To be added

## Supported Web Browsers

| Desktop Browsers |
| -------- |
| Safari 6.1+ |
| Google Chrome 32+ |
| Microsoft Edge |
| Firefox 27+ |

## Built With
- [Python](https://www.python.org)
- [PostgreSQL](https://www.postgresql.org/)
- [Apache Airflow](https://airflow.apache.org/)
- [BackboneJs](http://backbonejs.org/)

## Resources

* [Community Forum at TelecomHall.net](http://telecomHall.net)
* [Online Documentation](http://bts.bodastage.org)

## Copyright / License

Copyright 2017 - 2018 [Bodastage Solutions](http://www.bodastage.com)

Licensed under the Apache License, Version 2.0 ; you may not use this work except in compliance with the License. You may obtain a copy of the License in the LICENSE file, or at:

https://www.apache.org/licenses/LICENSE-2.0
