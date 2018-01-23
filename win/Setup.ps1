<#
 Setup.ps1
 
 This script installs Microsoft Hyper-V or Oracle VirtualBox, creates a docker machine and starts the services using docker-compose
 
 Emmanuel Robert Ssebaggala, Bodastage Solutions

 22/01/2018

 Licence: Apache 2.0
 
#>

$ScriptDir = Split-Path $script:MyInvocation.MyCommand.Path

$ScriptPath = $script:MyInvocation.MyCommand.Path

$BTSDir = (get-item $ScriptDir).parent.FullName

# Import functions file
. $ScriptDir"\Functions.ps1"

# Check if docker is installed 
$DockerIsInstalled = (docker version 2>&1 | Select-String "Version" | Measure-Object -Line | Select @{N="Installed"; E={$_.Lines -gt 0}} ).Installed


If( $DockerIsInstalled -ne "True" ){
	Write-Host "Docker is not installed"
	Write-Host "Download Docker for Windows from https://store.docker.com/editions/community/docker-ce-desktop-windows"
    Exit 1
}

# Add docker version to setup.log
docker version 2>&1 1> $BTSDir"\setup.log"

# Start Docker service if it is not running 
$StartStatus = StartService("*Docker*")

if( $StartStatus -eq "-1"){
    Write-Host "Docker service does not exits"
	Write-Host "Download the lastest Docker installer for Windows from https://store.docker.com/editions/community/docker-ce-desktop-windows"
	Write-Host ""
	Exit 1
}


Write-Host "Checking whether Microsoft Hyper-V is enabled..."
# Enable : Feature is enable , Disabled: Featrue is disabled, "":Feature is not available
$IsHyperVFeatureAvailable = (Get-WindowsOptionalFeature -FeatureName "Microsoft-Hyper-V" -Online).State

# If Hyper-V is not installed
if ( $IsHyperVFeatureAvailable -ne "Enabled" -and $IsHyperVFeatureAvailable -ne "Disabled"){
	Write-Host "Hyper-V is not installed."
	Write-Host ""
	# Exit 1
}

# Enable Hyper-V
if ( $IsHyperVFeatureAvailable -eq "Disabled" ){

	# Enable Hyper-V
	Enable-WindowsOptionalFeature –FeatureName "Microsoft-Hyper-V" –Online
	
	# Create virtual Switches
	Import-Module Hyper-V

	$ethernet = Get-NetAdapter -Name ethernet

	$wifi = Get-NetAdapter -Name wi-fi

	New-VMSwitch -Name BTSEthExternalSwitch -NetAdapterName $ethernet.Name -AllowManagementOS $true -Notes 'Parent OS, VMs, LAN'

	New-VMSwitch -Name BTSWiFiExternalSwitch -NetAdapterName $wifi.Name -AllowManagementOS $true -Notes 'Parent OS, VMs, wifi'

	New-VMSwitch -Name BTSPrivateSwitch -SwitchType Private -Notes 'Internal VMs only'
	
	# Create docker machine 
	# @TODO: Attach to the interface with a connection. For now, use the WiFiExternalSwitch
	docker-machine create -d hyperv -hyper-virtual-switch "BTSWiFiExternalSwitch" default
	
	# Create the containers 
	Write-Host "Creating and starting containers..."
    docker-compose up -d
	
	Write-Host "Setup completed"
	
	Exit 0
}

Write-Host "Should we continue the deployment with Oracle VirtualBox or wait for Microsoft Hyper-V to be installed?[y]"
$UseVirtualBox = read-host " y-continude with Oracle VirtualBox installation, n-Wait for Hyper-V to be installed"
Write-Host ""
if( $UseVirtualBox -eq "y" -Or $UseVirtualBox -eq "Y" -Or $UseVirtualBox -eq ""){
    Write-Host "Continuing deployment using Oracle VirtualBox..."
	Write-Host ""
}else{
    Write-Host "Install Microsoft-Hyper-V and run the setup again"
	Write-Host ""
    Exit 1
}


Write-Host "Checking whether Oracle VirtualBox is installed..."

# Check if VirtualBox is istalled
$IsVirtualBoxInstalled = Is-Installed("VirtualBox")

If( $IsVirtualBoxInstalled -eq "True" ){
    Write-Host "Oracle VirtualBox is installed."
	Write-Host ""
	
	Write-Host "Checking whether default machine exists..."
	
    # Check if default docker-machine exists
	$DockerMachineExist = (docker-machine ls | Select-String "default" | Measure-Object -Line | Select @{N="Exists"; E={$_.Lines -gt 0}} ).Exists
	
	if( $DockerMachineExist -eq  "True" ){
	    Write-Host "Default Docker machine exists"
		Write-Host ""
		
		Write-Host "Creating and starting up BTS-CE services..."
		docker-compose -d up  2>$null
		
		if( $LastExitCode -eq 0 ){
			Write-Host "BTS-CE successfully deployed and started."
			Write-Host ""
		}else{
		    Write-Host "B1TS-CE deployment failed. Contact support at TelecomHall.net"
			Write-Host ""
			Exit 1
		}
		
	}

}else{
	Write-Host "Download and install Oracle VirtualBox from https://www.virtualbox.org/wiki/Downloads and run the setup again"
	Write-Host ""
}


