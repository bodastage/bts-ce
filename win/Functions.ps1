
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


Function Execute-Command ($commandTitle, $commandPath, $commandArguments)
{
    $pinfo = New-Object System.Diagnostics.ProcessStartInfo
	#$pinfo.WindowStyle= [System.Diagnostics.ProcessWindowStyle]::Hidden
    $pinfo.FileName = $commandPath
    #$pinfo.RedirectStandardError = $true
    #$pinfo.RedirectStandardOutput = $true
    # $pinfo.UseShellExecute = $false
    $pinfo.Arguments = $commandArguments
	$pinfo.Verb = "runas"
    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $pinfo
    $p.Start() | Out-Null
    $p.WaitForExit()
    [pscustomobject]@{
        commandTitle = $commandTitle
        stdout = $p.StandardOutput.ReadToEnd()
        stderr = $p.StandardError.ReadToEnd()
        ExitCode = $p.ExitCode  
    }
}


Function Invoke-ElevatedCommand {
	<#
		.DESCRIPTION
			Invokes the provided script block in a new elevated (Administrator) powershell process, 
			while retaining access to the pipeline (pipe in and out). Please note, "Write-Host" output
			will be LOST - only the object pipeline and errors are handled. In general, prefer 
			"Write-Output" over "Write-Host" unless UI output is the only possible use of the information.
			Also see Community Extensions "Invoke-Elevated"/"su"

		.EXAMPLE
			Invoke-ElevatedCommand {"Hello World"}

		.EXAMPLE
			"test" | Invoke-ElevatedCommand {$input | Out-File -filepath c:\test.txt}

		.EXAMPLE
			Invoke-ElevatedCommand {$one = 1; $zero = 0; $throwanerror = $one / $zero}

		.PARAMETER Scriptblock
			A script block to be executed with elevated priviledges.

		.PARAMETER InputObject
			An optional object (of any type) to be passed in to the scriptblock (available as $input)

		.PARAMETER EnableProfile
			A switch that enables powershell profile loading for the elevated command/block

		.PARAMETER DisplayWindow
			A switch that enables the display of the spawned process (for potential interaction)

		.SYNOPSIS
			Invoke a powershell script block as Administrator

		.NOTES
			Originally from "Windows PowerShell Cookbook" (O'Reilly), by Lee Holmes, September 2010
				at http://poshcode.org/2179
			Modified by obsidience for enhanced error-reporting, October 2010
				at http://poshcode.org/2297
			Modified by Tao Klerks for code formatting, enhanced/fixed error-reporting, and interaction/hanging options, January 2012
				at https://gist.github.com/gists/1582185
				and at http://poshcode.org/, followup to http://poshcode.org/2297
			SEE ALSO: "Invoke-Elevated" (alias "su") in PSCX 2.0 - simpler "just fire" elevated process runner.
	#>

	param
	(
		## The script block to invoke elevated. NOTE: to access the InputObject/pipeline data from the script block, use "$input"!
		[Parameter(Mandatory = $true)]
		[ScriptBlock] $Scriptblock,
	 
		## Any input to give the elevated process
		[Parameter(ValueFromPipeline = $true)]
		$InputObject,
	 
		## Switch to enable the user profile
		[switch] $EnableProfile,
	 
		## Switch to display the spawned window (as interactive)
		[switch] $DisplayWindow
	)
	 
	begin
	{
		Set-StrictMode -Version Latest
		$inputItems = New-Object System.Collections.ArrayList
	}
	 
	process
	{
		$null = $inputItems.Add($inputObject)
	}
	 
	end
	{

		## Create some temporary files for streaming input and output
		$outputFile = [IO.Path]::GetTempFileName()	
		$inputFile = [IO.Path]::GetTempFileName()
		$errorFile = [IO.Path]::GetTempFileName()

		## Stream the input into the input file
		$inputItems.ToArray() | Export-CliXml -Depth 1 $inputFile
	 
		## Start creating the command line for the elevated PowerShell session
		$commandLine = ""
		if(-not $EnableProfile) { $commandLine += "-NoProfile " }

		if(-not $DisplayWindow) { 
			$commandLine += "-Noninteractive " 
			$processWindowStyle = "Hidden" 
		}
		else {
			$processWindowStyle = "Normal" 
		}
	 
		## Convert the command into an encoded command for PowerShell
		$commandString = "Set-Location '$($pwd.Path)'; " +
			"`$output = Import-CliXml '$inputFile' | " +
			"& {" + $scriptblock.ToString() + "} 2>&1 ; " +
			"Out-File -filepath '$errorFile' -inputobject `$error;" +
			"Export-CliXml -Depth 1 -In `$output '$outputFile';"
	 
		$commandBytes = [System.Text.Encoding]::Unicode.GetBytes($commandString)
		$encodedCommand = [Convert]::ToBase64String($commandBytes)
		$commandLine += "-EncodedCommand $encodedCommand"

		## Start the new PowerShell process
		$process = Start-Process -FilePath (Get-Command powershell).Definition `
			-ArgumentList $commandLine `
			-Passthru `
			-Verb RunAs `
			-WindowStyle $processWindowStyle

		$process.WaitForExit()

		$errorMessage = $(gc $errorFile | Out-String)
		if($errorMessage) {
			Write-Error -Message $errorMessage
		}
		else {
			## Return the output to the user
			if((Get-Item $outputFile).Length -gt 0)
			{
				Import-CliXml $outputFile
			}
		}

		## Clean up
		Remove-Item $outputFile
		Remove-Item $inputFile
		Remove-Item $errorFile
	}
}