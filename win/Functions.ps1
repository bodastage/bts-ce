
<#
Get a list of all installed programs
#>
function Get-InstalledApps
{
    if ([IntPtr]::Size -eq 4) {
        $regpath = 'HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*'
    }
    else {
        $regpath = @(
            # 'HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*'
            'HKLM:\Software\Wow6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*'
        )
    }
	
    Get-ItemProperty $regpath | .{process{if($_.DisplayName -and $_.UninstallString) { $_ } }} | Select DisplayName, Publisher, InstallDate, DisplayVersion, UninstallString |Sort DisplayName
}


<#
Check if a program is installed 

@param String $program The name of the program to check for
@return Boolean Returns true if a program matching the specified name is installed.
#>
function Is-Installed( $program ) {
    
    $x86 = ((Get-ChildItem "HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall") |
        Where-Object { $_.GetValue( "DisplayName" ) -like "*$program*" } ).Length -gt 0;

    $x64 = ((Get-ChildItem "HKLM:\Software\Wow6432Node\Microsoft\Windows\CurrentVersion\Uninstall") |
        Where-Object { $_.GetValue( "DisplayName" ) -like "*$program*" } ).Length -gt 0;

    return $x86 -or $x64;
}

<#
Start and wait for service to start

@param String $ServiceName The name of the service
@return Integer 0-Success, -1-No service with given name, 1-Failed to start service
#>
function StartService( $ServiceName){
	$arrService = Get-Service -Name $ServiceName

	If ( !$arrService ){
		return "-1"
	}
	while ($arrService.Status -ne 'Running')
	{

		Start-Service $ServiceName
		write-host $arrService.status
		write-host 'Service starting'
		Start-Sleep -seconds 60
		$arrService.Refresh()
		if ($arrService.Status -eq 'Running')
		{
			Write-Host 'Service is now Running'
		}

	}
	
	return "0"
}