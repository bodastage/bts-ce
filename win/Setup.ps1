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

# Driver's to use to create container VMs
$UseHyperVDriver=$False
$UseVirtualBoxDriver=$True

Write-Host  -NoNewline "Checking whether hardware virtualization is supported and enabled..."
#Check if Hardware Virtualization is supported and enabled
$IsVTSupportedAndEnabled = (GWMI Win32_Processor).VirtualizationFirmwareEnabled
Write-host -NoNewline $(If ($IsVTSupportedAndEnabled -ne $True -and $IsVTSupportedAndEnabled -ne $False) {"Not Supported"} ) 
Write-host -NoNewline $(If ($IsVTSupportedAndEnabled -eq $True ) {"Enabled"} ElseIf ($IsVTSupportedAndEnabled -eq $False){"Disabled" } ) 
Write-Host ""
Write-Host ""

Write-Host  -NoNewline "Checking whether Microsoft Hyper-V is available and enabled..."
# Enable : Feature is enable , Disabled: Featrue is disabled, "":Feature is not available
$IsHyperVFeatureAvailable = (Get-WindowsOptionalFeature -FeatureName "Microsoft-Hyper-V" -Online).State
Write-host -NoNewline $(If ($IsHyperVFeatureAvailable -ne $True -and $IsHyperVFeatureAvailable -ne $False) {"Not available"} ) 
Write-host -NoNewline $(If ($IsHyperVFeatureAvailable -eq $True) {"Enabled"} ElseIf ($IsHyperVFeatureAvailable -eq $False){"Disabled" } ) 
Write-Host ""
Write-Host ""

# If VT is disabled and HyperV is Disabled, ask user to enabled them before proceeding or continue with VB 
if($IsVTSupportedAndEnabled -eq $False -and $IsHyperVFeatureAvailable -eq $False){
		Write-Host "Hardware virtualization and Microsoft-Hyper-V are disabled."
		Write-Host "Can we continue with Oracle VirtualBox?[y] Or stop the setup while you enable them?"
		$continueWithVB = read-host " y-continude with Oracle VirtualBox installation, n-Let me try to enabled them."
		if( $continueWithVB -eq "y"){
			$UseHyperVDriver=$False
		}else{
		    Write-Host "Hardware virtualization is enabled from the BIOS. Run the setup when done"
			Write-Host "God speed!"
			# Stop here while user tries to enable Hardware virtualization and Microsoft-Hyper-V
			Exit 1
		}
}


# If VT is enabled and HyperV is Disabled, enabled HyperV 
if($IsVTSupportedAndEnabled -eq $True -and $IsHyperVFeatureAvailable -eq $False){
    # Enable Hyper-V
	Enable-WindowsOptionalFeature -Online -FeatureName:Microsoft-Hyper-V -All
	
	# Hyper-V successfully enabled
	If($LastExitCode -eq 0 ){
		$UseHyperVDriver=$True
	}else{
		Write-Host "Failed to enabled Microsoft-Hyper-V"
		Write-Host "Can we continue with Oracle VirtualBox?[y] Or stop here while you enable it on your own?"
		$continueWithVB = read-host " y-continude with Oracle VirtualBox installation, n-Let me try to enabled it."
		if( $continueWithVB -eq "y"){
			$UseHyperVDriver=$False
		}else{
			# Stop here while user tries to enable Microsoft-Hyper-V
			Exit 1
		}
	}
}

# If VT-X is supported but Hyper-V is not availabled
If( $IsVTSupportedAndEnabled -ne $Null -and $IsHyperVFeatureAvailable -eq $Null){
	Write-Host "Continuing setup with Oracle VirtualBox..."
	Write-Host ""
	$UseHyperVDriver=$False
}

#If VT-X is support but is not enabled  
If( $IsVTSupportedAndEnabled -eq $Null -and $IsHyperVFeatureAvailable -eq $Null){
	Write-Host "Hardware virtualization is not supported by your processor and Microsoft-Hyper-V is not available"
	Write-Host "Setup will continue with Oracle VirtualBox"
	Write-Host ""
	$UseHyperVDriver=$False
}

# Let's setup HyperV 
if ( $UseHyperVDriver -eq $True ){

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


$InstallVB = $False
# Let's use Oracle VirtualBox 
if( $UseHyperVDriver -eq $False ){
	
	# Check if VB is already intallled 
	Write-Host -NoNewline "Checking whether Oracle VirtualBox is installed..."
	$IsVirtualBoxInstalled = Is-Installed("VirtualBox")
    If( $IsVirtualBoxInstalled -eq $True){
		Write-Host "Installed."
	}else{
	    Write-Host "No installed"
		$InstallVB = $True
	}
	Write-Host ""
}

# Install Oracle VirtualBox
If($InstallVB -eq $True){
	$fileURI = 'https://download.virtualbox.org/virtualbox/5.2.6/VirtualBox-5.2.6-120293-Win.exe';
	$outputFile = $ScriptDir+"\OracleVirtualBox.exe"
	
	Write-Host "Downloading Oracle VirtualBox from https://www.virtualbox.org..."
	(New-Object System.Net.WebClient).DownloadFile($fileURI, $outputFile)
	
	if($LastExitCode -ne 0){
		Write-Host "Download failed. Check your network connectivity"
		Write-Host "Or download and install Oracle VirtualBox from https://download.virtualbox.org/"
		Write-Host ""
		Exit 1
	}else{
		Write-Host "Download completed."
	}
	
	#Install VirtualBox
	
}

Exit 0


# Check if docker is installed 
$IsDockerInstalled = (docker version 2>&1 | Select-String "Version" | Measure-Object -Line | Select @{N="Installed"; E={$_.Lines -gt 0}} ).Installed

If( $IsDockerInstalled -ne $True ){
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


