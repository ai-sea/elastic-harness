$ErrorActionPreference = 'Stop'

$env:GOCACHE = Join-Path $env:TEMP 'elastic-harness-go-cache'
$outputDirectory = Join-Path $PSScriptRoot '..\bin'
New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null

Push-Location (Join-Path $PSScriptRoot '..\apps\standalone-app')
try {
    go build -trimpath -o (Join-Path $outputDirectory 'elastic-harness.exe') .
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
finally {
    Pop-Location
}

Write-Host "已生成 bin/elastic-harness.exe"
