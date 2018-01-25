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
Write-host -NoNewline $(If ($IsVTSupportedAndEnabled -eq $Null ) {"Not Supported"} ) 
Write-host -NoNewline $(If ($IsVTSupportedAndEnabled -eq $True ) {"Enabled"} ElseIf ($IsVTSupportedAndEnabled -eq $False){"Disabled" } ) 
Write-Host ""
Write-Host ""

Write-Host  -NoNewline "Checking whether Microsoft Hyper-V is available and enabled..."
# Enable : Feature is enable , Disabled: Featrue is disabled, "":Feature is not available
$IsHyperVFeatureEnabled = (Get-WindowsOptionalFeature -FeatureName "Microsoft-Hyper-V" -Online).State
Write-host -NoNewline $(If ($IsHyperVFeatureEnabled -eq $Null) {"Not available"}else{ $IsHyperVFeatureEnabled } ) 
Write-Host ""
Write-Host ""

# If VT is disabled and HyperV is Disabled, ask user to enabled them before proceeding or continue with VB 
if($IsVTSupportedAndEnabled -eq $False -and $IsHyperVFeatureEnabled -eq "Disabled"){
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
if($IsVTSupportedAndEnabled -eq $True -and $IsHyperVFeatureEnabled -eq "Disabled"){
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
If( $IsVTSupportedAndEnabled -ne $Null -and $IsHyperVFeatureEnabled -eq $Null){
	Write-Host "Continuing setup with Oracle VirtualBox."
	Write-Host ""
	$UseHyperVDriver=$False
}

#If VT-X is support but is not enabled  
If( $IsVTSupportedAndEnabled -eq $Null -and $IsHyperVFeatureEnabled -eq $Null){
	Write-Host "Hardware virtualization is not supported by your processor and Microsoft-Hyper-V is not available"
	Write-Host "Setup will continue with Oracle VirtualBox"
	Write-Host ""
	$UseHyperVDriver=$False
}

# Let's setup HyperV 
if ( $UseHyperVDriver -eq $True ){

	#Download and install Docker for Windows
	$DockerForWindowsURI = "https://download.docker.com/win/stable/Docker%20for%20Windows%20Installer.exe"
	$DFWInstaller = $BTSDir + "\" + "Docker for Windows Installer.exe"
	Write-Host "Downloading Docker for Windows..."
	(New-Object System.Net.WebClient).DownloadFile($DockerForWindowsURI, $DFWInstaller)
	if($LastExitCode -ne 0){
		Write-Host "Failed."
		Write-Host -NoNewline "Check your network connectivity. "
		Write-Host "Or download and install Docker Toolbox from https://download.docker.com/win/stable/Docker%20for%20Windows%20Installer.exe"
		Write-Host ""
		Exit 1
	}else{
		Write-Host "Completed."
		Write-Host ""
	}
	
	#Install Docker for Windows
	Write-Host -NoNewline "Installing Docker for Windows..."
	Start-Process -wait -FilePath $DFWInstaller -ArgumentList "/VERYSILENT LOG $ScriptDir+'\DockerForWindows.log'  /SP /NOCANCEL /NORESTART /CLOSEAPPLICATIONS /RESTARTAPPLICATIONS"
	# @TODO: Check status of installation before continuing
	Write-Host "Done"
	
	# Create virtual Switches
	Import-Module Hyper-V

	# Get active network connection by taking the first active physical interface
	$activeInterfeceName = $(Get-NetAdapter -physical  | where status -eq 'up' | Select-Object -first 1).Name

	New-VMSwitch -Name BTSExternalSwitch -NetAdapterName $activeInterfeceName -AllowManagementOS $true -Notes 'BTS External Switch'

	New-VMSwitch -Name BTSPrivateSwitch -SwitchType Private -Notes 'Internal VMs only'

	# Create docker machine 
	# @TODO: Attach to the interface with a connection. For now, use the WiFiExternalSwitch
	Write-Host -NoNewline "Creating docker-machine..."
	docker-machine create -d hyperv -hyper-virtual-switch "BTSWiFiExternalSwitch" default
	Write-Host "Done"
	Write-Host ""
	
	# Create the containers 
	Write-Host "Creating and starting containers..."
    docker-compose up -d
	
	Write-Host "Setup completed"
	
	Exit 0
}





<# 
# Docker Toolbox automatically install virtualbox so this section is not needed

$InstallVB = $False
# Let's use Oracle VirtualBox if HyperV can't be used
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
	$VBVersion="5.2.6"
	$VBRevision="120293"
	$VBInstaller="VirtualBox-"+$VBVersion+"-"+$VBRevision+"-Win.exe"
	$fileURI = "https://download.virtualbox.org/virtualbox/"+$VBVersion+"/" + $VBInstaller
	$outputFile = $ScriptDir+"\"+$VBInstaller
	
	Write-Host "Downloading Oracle VirtualBox from https://www.virtualbox.org..."
	(New-Object System.Net.WebClient).DownloadFile($fileURI, $outputFile)
	
	if($LastExitCode -ne 0){
		Write-Host "Download failed. Check your network connectivity. "
		Write-Host "Or download and install Oracle VirtualBox from https://download.virtualbox.org/"
		Write-Host ""
		Exit 1
	}else{
		Write-Host "Download completed."
	}
	
	# Install VirtualBox
	Write-Host -NoNewline "Installing Oracle VirtualBox..."
	Start-Process -wait -FilePath $outputFile -ArgumentList "--extract --silent"
	Start-Process msiexec.exe -ArgumentList "/i  $($Env:Temp + "\VirtualBox\VirtualBox-" + $VBVersion + '-r' + $VBRevision + '-MultiArch_amd64.msi') /passive /norestart" -Wait
	Remove-Item –path $($Env:Temp + "\VirtualBox") -recurse
	Write-Host "Done"
	Write-Host ""
}#>

# Install Docker toolbox
# First check if Docker tookbox is already instead 
Write-Host -NoNewline "Checking if Docker Toolbox is installed..."
$IsDockerToolBoxInstalled = Is-Installed("Docker Toolbox")
if($IsDockerToolBoxInstalled -eq $False){
	Write-Host "Not Installed"
	Write-Host ""
	
	# Download and Install Docker Toolbox
	$DockerToolboxURI = "https://download.docker.com/win/stable/DockerToolbox.exe"
	$DockerToolboxInstaller = $BTSDir + "\" + "DockerToolbox.exe"
	Write-Host -NoNewline "Downloading Docker Toolbox..."
	(New-Object System.Net.WebClient).DownloadFile($DockerToolboxURI, $DockerToolboxInstaller)
	
	if($LastExitCode -ne 0){
		Write-Host "Failed."
		Write-Host -NoNewline "Check your network connectivity. "
		Write-Host "Or download and install Docker Toolbox from https://download.docker.com/win/stable/DockerToolbox.exe"
		Write-Host ""
		Exit 1
	}else{
		Write-Host "Completed."
		Write-Host ""
	}
	
	#Install Docker ToolBox
	Write-Host -NoNewline "Installing Docker Toolbox..."
	Start-Process -wait -FilePath $DockerToolboxInstaller -ArgumentList "/VERYSILENT LOG $ScriptDir+'\DockerToolbox.log'  /SP /NOCANCEL /NORESTART /CLOSEAPPLICATIONS /RESTARTAPPLICATIONS"
	Write-Host "Done"
	
    # Create docker machine 
	Write-Host "Creating default docker-machine..."
	docker-machine create -d virtualbox default
	Write-Host "Done"
	Write-Host ""
	
	# Create the containers 
	Write-Host "Creating and starting containers..."
    docker-compose up -d
	
	Write-Host "Setup completed"
}else{
	Write-Host "Installed"
	Write-Host ""
}
