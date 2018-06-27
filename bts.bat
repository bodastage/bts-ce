@Echo Off
Rem 
Rem Copyright 2018 Bodastage Solutions
Rem
Rem Licensed under the Apache License, Version 2.0 (the "License");
Rem you may not use this file except in compliance with the License.
Rem You may obtain a copy of the License at
Rem
Rem     http://www.apache.org/licenses/LICENSE-2.0
Rem
Rem Unless required by applicable law or agreed to in writing, software
Rem distributed under the License is distributed on an "AS IS" BASIS,
Rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
Rem See the License for the specific language governing permissions and
Rem limitations under the License.
Rem
Rem 
Rem Date 22/01/2018
Rem Author Emmanuel Robert Ssebaggala <emmanuel.ssebaggala@bodastage.com> 


Rem Check if script is running in an elevated terminal 
net session >nul 2>&1
If %errorLevel% == 0 (
   Echo Do not run as an Administrator
   Exit /b 1
)

Rem Test if docker-machine is running
@For /F "tokens=* USEBACKQ" %%F In (`docker-machine status default 2^>Nul`) Do (
	Set Status=%%F
	If Not "%Status%" == "Running" (
		docker-machine start default
	)
)

Rem Set docker env variables . Add so that docker commands can be run from cmd
Rem Review this later.
@FOR /f "tokens=*" %%i In ('docker-machine env --shell=cmd 2^>Nul') Do @%%i

Rem application root directory 
For /F %%i In ("%~dp0") Do Set BD_ROOT_DIR=%%~fi
cd %BD_ROOT_DIR%

Rem Status
Rem -------------------------
If "%~1"==""  (
 
	Echo Boda Telecom Suite CE - Management Utility
	Echo -----------------------------------------------------
	Echo bts version                        -- Application version
	Echo bts setup                          -- Setup application, create and start services
	Echo bts start [service_name]           -- Start application services
	Echo bts stop [service_name]            -- Stop application
	Echo bts status                         -- See process statuses
	Echo bts logs [service_name]            -- See logs from containers
	Echo bts images                         -- See images
	Echo bts rm [service_name]              -- Stop and remove
	Echo bts create [service_name]          -- Create services
	Echo bts ps [service_name]              -- Display running processes
	Echo bts pause [service_name]           -- Pause services
	Echo bts unpause [service_name]         -- Pause services
	Echo bts exec service_name command      -- Run command in service's container
	Echo bts recreate                       -- Re-create the services. Useful when working with a version update
	Echo.
	Rem Echo manage upgrage -- Upgrade
	Rem Echo manage list modules -- List installed modules
	Echo -----------------------------------------------------
	Echo Boda Telecom Suite - Community Edition
	Echo Copyright 2017-2018. Bodastage Solutions. http://www.bodastage.com
)

Rem Run setup 
Rem -------------------------
If "%~1"=="setup" ( 
    Echo Running BTS-CE setup...
	Echo. 
	Powershell -ExecutionPolicy ByPass -File win\Setup.ps1
)

Rem start 
Rem -------------------------
If "%~1"=="start" ( 
    Rem start machine 
	Rem docker-machine start > Nul
    Rem docker-compose start
	
    if Not "%~2" == "" (
	    docker-compose start %~2
		Exit /b 0
	)
	
    docker-compose start
)

Rem Status 
Rem -------------------------
If "%~1"=="status" ( 
    docker container ls
)

Rem restart 
Rem -------------------------
If "%~1"=="restart" ( 
    docker-compose restart
)

Rem version 
Rem -------------------------
If "%~1"=="version" ( 
    Rem This duplication is intentional. Needs to be fixed though. The version is only picked on the second call!
    Set /p Release=<VERSION
	Set /p Release=<VERSION 
    Echo Version:%Release%
	Echo Boda Telecom Suite - Community Edition
	Echo Copyright 2017-2018. Bodastage Solutions. http://www.bodastage.com
)


Rem stop 
Rem -------------------------
If "%~1"=="stop" ( 
    if Not "%~2" == "" (
	    docker-compose stop %~2
		Exit /b 0
	)
	
    docker-compose stop
	Rem docker-machine stop
)

Rem logs 
Rem -------------------------
If "%~1"=="logs" ( 

    if Not "%~2" == "" (
	    docker-compose logs %~2
		Exit /b 0
	)
	
    docker-compose logs
)


Rem  exit /b 0

Rem images 
Rem -------------------------
If "%~1"=="images" ( 
    docker-compose images
)

Rem remove  
Rem -------------------------
If "%~1"=="rm" ( 

    if Not "%~2" == "" (
	    docker-compose stop %~2
		docker-compose rm -f %~2
		Exit /b 0
	)
	
    docker-compose stop
    docker-compose rm -f
	docker volume prune
)

Rem Create container and start service  
Rem -------------------------
If "%~1"=="create" ( 

    if Not "%~2" == "" (
	    docker-compose up -d %~2
		Exit /b 0
	)
	
    docker-compose up -d
)


Rem Display running processes
Rem -------------------------
If "%~1"=="ps" ( 

    if Not "%~2" == "" (
	    docker-compose top %~2
		Exit /b 0
	)
	
)

Rem Pause services
Rem -------------------------
If "%~1"=="pause" ( 

    if Not "%~2" == "" (
	    docker-compose pause %~2
		Exit /b 0
	)
	
    docker-compose pause
)


Rem un pause services
Rem -------------------------
If "%~1"=="unpause" ( 

    if Not "%~2" == "" (
	    docker-compose unpause %~2
		Exit /b 0
	)
	
    docker-compose unpause
)

Rem  Exec
Rem -------------------------
If "%~1"=="exec" ( 

    if Not "%~2" == ""  (
	    docker-compose %*
		Exit /b 0
	)
	
    docker-compose unpause
)

Rem  Recreate services
Rem -------------------------
If "%~1"=="recreate" ( 
    @FOR /f "tokens=*" %%i In ('docker ps -q 2^>Nul') Do ( docker stop %%i & docker rm %%i )
	bts create
)