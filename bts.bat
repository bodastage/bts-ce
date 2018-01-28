@Echo Off
Rem Management sript
Rem 
Rem Manage application
Rem 
Rem Date 22/01/2018
Rem Author Emmanuel Robert Ssebaggala <emmanuel.ssebaggala@bodastage.com> 


Rem Check if script is running in an elevated terminal 
net session >nul 2>&1
If %errorLevel% == 0 (
   Echo Do not run as an Administrator
   Exit /b 1
)


Rem Set docker env variables . Add so that docker commands can be run from cmd
Rem Review this later.
@FOR /f "tokens=*" %%i In ('docker-machine env 2^>Nul') Do @%%i


Rem application root directory 
For /F %%i In ("%~dp0") Do Set BD_ROOT_DIR=%%~fi
cd %BD_ROOT_DIR%

Rem Status
Rem -------------------------
If "%~1"==""  (
 
	Echo Boda Telecom Suite CE - Management Utility
	Echo -----------------------------------------------------
	Echo bts version           -- Application version
	Echo bts setup             -- Setup application
	Echo bts start             -- Start application services
	Echo bts stop              -- Stop application
	Echo bts status            -- See process statuses
	Echo bts logs              -- See logs from containers
	Echo bts images            -- See images
	Echo bts rm                -- Stop and remove
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
	docker-machine start > Nul
	
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
    Echo Version: 1.0.17
	Echo Boda Telecom Suite - Community Edition
	Echo Copyright 2017-2018. Bodastage Solutions. http://www.bodastage.com
)


Rem stop 
Rem -------------------------
If "%~1"=="stop" ( 
    docker-compose stop
)

Rem logs 
Rem -------------------------
If "%~1"=="logs" ( 
    docker-compose logs
)


Rem  exit /b 0

Rem images 
Rem -------------------------
If "%~1"=="images" ( 
    docker-compose images
)

Rem images 
Rem -------------------------
If "%~1"=="rm" ( 

    if "%~2" == "" (
	    docker-compose stop %~2
		docker-compose rm -f %~2
		Exit /b 0
	)
	
    docker-compose stop
    docker-compose rm -f
)
