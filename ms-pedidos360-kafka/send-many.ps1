#!/usr/bin/env pwsh
#
# send-many.ps1 - Envía muchas peticiones al endpoint de Kafka de Pedidos360
#                (microservicio ms-pedidos360-kafka, guía 3.2.1) en Windows/PowerShell.
#
# Uso básico:
#   .\send-many.ps1                     # 100 mensajes, 1 petición a la vez
#   .\send-many.ps1 -Count 500          # 500 mensajes
#   .\send-many.ps1 -Count 1000 -Parallel 20
#
# Parámetros:
#   -Count       Cantidad de mensajes          (default: 100)
#   -Parallel    Peticiones simultáneas        (default: 1, requiere PowerShell 7+)
#   -Url         Endpoint destino              (default: http://localhost:8888/api/kafka/send)
#   -KeyPool     Cantidad de keys distintas    (default: 12)
#   -KeyPrefix   Prefijo de la key             (default: orden)
#   -MsgPrefix   Prefijo del mensaje           (default: mensaje)
#   -Method      GET o POST                    (default: POST)
#   -Timeout     Timeout por petición (seg)    (default: 15)
#   -VerboseOut  Muestra cada petición
#
# Ejemplos:
#   .\send-many.ps1 -Count 2000 -Parallel 10
#   .\send-many.ps1 -Count 300 -KeyPool 3
#   .\send-many.ps1 -Count 50 -Method GET
#
[CmdletBinding()]
param(
    [int]$Count = 100,
    [int]$Parallel = 1,
    [string]$Url = 'http://localhost:8888/api/kafka/send',
    [int]$KeyPool = 12,
    [string]$KeyPrefix = 'orden',
    [string]$MsgPrefix = 'mensaje',
    [ValidateSet('GET', 'POST')]
    [string]$Method = 'POST',
    [int]$Timeout = 15,
    [switch]$VerboseOut
)

if ($Count -lt 1) { Write-Error 'Count debe ser mayor que 0'; exit 2 }
if ($Parallel -lt 1) { Write-Error 'Parallel debe ser mayor que 0'; exit 2 }
if ($KeyPool -lt 1) { Write-Error 'KeyPool debe ser mayor que 0'; exit 2 }

# Comprueba que el endpoint esté disponible antes de empezar (sin enviar mensajes).
try {
    Invoke-WebRequest -Uri $Url -Method GET -UseBasicParsing -TimeoutSec 5 | Out-Null
} catch {
    $probe = 0
    if ($_.Exception.Response) { $probe = [int]$_.Exception.Response.StatusCode }
    if ($probe -eq 0) {
        Write-Host "ERROR: el endpoint $Url no responde." -ForegroundColor Red
        Write-Host '       Verifica que la app esté corriendo y que Kafka esté arriba:' -ForegroundColor Red
        Write-Host '       docker compose up -d zookeeper kafka' -ForegroundColor Red
        Write-Host '       cd ms-pedidos360-kafka; .\mvnw spring-boot:run' -ForegroundColor Red
        exit 1
    }
}

function Send-One {
    param([int]$Index, [string]$BaseUrl, [string]$HttpMethod, [int]$TimeoutSec, [string]$Key, [string]$Message)
    $uri = $BaseUrl + '?message=' + [uri]::EscapeDataString($Message) + '&key=' + [uri]::EscapeDataString($Key)
    try {
        $resp = Invoke-WebRequest -Uri $uri -Method $HttpMethod -UseBasicParsing -TimeoutSec $TimeoutSec
        return [int]$resp.StatusCode
    } catch {
        if ($_.Exception.Response) { return [int]$_.Exception.Response.StatusCode }
        return 0
    }
}

$results = [System.Collections.Concurrent.ConcurrentBag[object]]::new()
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

Write-Host "Enviando $Count mensajes a $Url"
Write-Host "Método: $Method | Paralelismo: $Parallel | Keys distintas: $KeyPool"
Write-Host ''

$useParallel = ($Parallel -gt 1) -and ($PSVersionTable.PSVersion.Major -ge 7)

if ($useParallel) {
    1..$Count | ForEach-Object -Parallel {
        $i = $_
        $key = '{0}-{1}' -f $using:KeyPrefix, (($i - 1) % $using:KeyPool)
        $msg = '{0}-{1}' -f $using:MsgPrefix, $i
        $uri = $using:Url + '?message=' + [uri]::EscapeDataString($msg) + '&key=' + [uri]::EscapeDataString($key)
        try {
            $resp = Invoke-WebRequest -Uri $uri -Method $using:Method -UseBasicParsing -TimeoutSec $using:Timeout
            $code = [int]$resp.StatusCode
        } catch {
            if ($_.Exception.Response) { $code = [int]$_.Exception.Response.StatusCode } else { $code = 0 }
        }
        ($using:results).Add([pscustomobject]@{ Code = $code; Index = $i; Key = $key })
        if ($using:VerboseOut) { Write-Host ("{0}`t{1}`t{2}" -f $code, $i, $key) }
    } -ThrottleLimit $Parallel
} else {
    if ($Parallel -gt 1) {
        Write-Warning 'Paralelismo > 1 requiere PowerShell 7+. Se ejecutará secuencialmente.'
    }
    foreach ($i in 1..$Count) {
        $key = '{0}-{1}' -f $KeyPrefix, (($i - 1) % $KeyPool)
        $msg = '{0}-{1}' -f $MsgPrefix, $i
        $code = Send-One -Index $i -BaseUrl $Url -HttpMethod $Method -TimeoutSec $Timeout -Key $key -Message $msg
        $results.Add([pscustomobject]@{ Code = $code; Index = $i; Key = $key })
        if ($VerboseOut) { Write-Host ("{0}`t{1}`t{2}" -f $code, $i, $key) }
    }
}

$stopwatch.Stop()
$all = $results.ToArray()
$ok = @($all | Where-Object { $_.Code -eq 200 }).Count
$fail = $Count - $ok
$elapsed = [math]::Max([int][math]::Round($stopwatch.Elapsed.TotalSeconds), 1)

if (-not $VerboseOut) {
    $all | Where-Object { $_.Code -ne 200 } |
        ForEach-Object { Write-Host ("FALLO  HTTP {0}  mensaje={1}  key={2}" -f $_.Code, $_.Index, $_.Key) -ForegroundColor Red }
}

Write-Host ''
Write-Host '----------------------------------------'
Write-Host "Total enviado : $Count"
Write-Host "OK (HTTP 200) : $ok"
Write-Host "Fallidos      : $fail"
Write-Host "Tiempo        : ${elapsed}s (~$([int]($Count / $elapsed)) msg/s)"
Write-Host '----------------------------------------'

if ($fail -gt 0) { exit 1 }
