Rem Taken from https://social.technet.microsoft.com/Forums/windows/en-US/05cce5f6-3c3a-4bb8-8b72-8c1ce4b5eff1/how-to-run-a-program-as-adminitrator-via-the-command-line?forum=w7itproappcompat
Rem 
@echo Set objShell = CreateObject("Shell.Application") > %temp%\sudo.tmp.vbs
@echo args = Right("%*", (Len("%*") - Len("%1"))) >> %temp%\sudo.tmp.vbs
@echo objShell.ShellExecute "%1", args, "", "runas" >> %temp%\sudo.tmp.vbs
@cscript %temp%\sudo.tmp.vbs