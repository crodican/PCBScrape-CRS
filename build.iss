[Setup]
AppName=Recovery Specialist Data Tool
AppVersion=1.0
DefaultDirName={pf}\RecoverySpecialist
DefaultGroupName=Recovery Specialist
OutputBaseFilename=RecoverySpecialistSetup
Compression=lzma
SolidCompression=yes

[Files]
Source: "dist\pa_recovery_pyqt5.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "dist\icon.ico"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\Recovery Specialist Data Tool"; Filename: "{app}\pa_recovery_pyqt5.exe"; IconFilename: "{app}\icon.ico"
Name: "{userdesktop}\Recovery Specialist Data Tool"; Filename: "{app}\pa_recovery_pyqt5.exe"; IconFilename: "{app}\icon.ico"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop icon"; GroupDescription: "Additional icons:"