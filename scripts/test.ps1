$ErrorActionPreference = 'Stop'

# 使用工作区外缓存，避免在仓库产生构建临时文件。
$env:GOCACHE = Join-Path $env:TEMP 'elastic-harness-go-cache'
$env:GOPROXY = 'off'
$env:GOSUMDB = 'off'

$modules = @(
    'core',
    'components/harness-definition',
    'components/execution-profile',
    'components/handler-registry',
    'components/onstate-runtime',
    'components/effect-dispatcher',
    'components/event-projector',
    'components/timer-reconciler',
    'components/tool-registry',
    'components/llm-handler',
    'components/tool-handler',
    'components/tool-executor',
    'components/api',
    'adapters/kv-sqlite',
    'adapters/ceq-embedded',
    'adapters/seq-embedded',
    'adapters/objectstore-local',
    'adapters/modelprovider-openai',
    'adapters/toolexecutor-http',
    'apps/standalone-app'
)

foreach ($module in $modules) {
    Write-Host "==> 测试 $module"
    Push-Location $module
    try {
        go test ./...
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        go vet ./...
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    finally {
        Pop-Location
    }
}
