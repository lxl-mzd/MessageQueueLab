# e2e smoke test for docker-compose cluster (ASCII only, safe for CI pwsh)
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$Base = "http://127.0.0.1:"
$Ports = @(5081, 5082, 5083)
$Names = @{ 5081 = "mq-leader"; 5082 = "mq-follower-1"; 5083 = "mq-follower-2" }
$script:Fails = 0

function Http([string]$u, [string]$m = "POST", [string]$b = $null) {
    try {
        if ($b) {
            $r = Invoke-WebRequest -Uri $u -Method $m -Body $b -ContentType "application/json" -UseBasicParsing -TimeoutSec 25
            return @{ code = [int]$r.StatusCode; body = $r.Content }
        }
        $r = Invoke-WebRequest -Uri $u -Method $m -UseBasicParsing -TimeoutSec 25
        return @{ code = [int]$r.StatusCode; body = $r.Content }
    } catch {
        $code = 0; $txt = ""
        if ($_.Exception.Response) {
            $code = [int]$_.Exception.Response.StatusCode
            $sr = New-Object IO.StreamReader($_.Exception.Response.GetResponseStream()); $txt = $sr.ReadToEnd()
        }
        return @{ code = $code; body = $txt }
    }
}
function Assert([string]$name, [bool]$ok, [string]$detail = "") {
    if ($ok) { Write-Output "PASS $name" }
    else { $script:Fails++; Write-Output "FAIL $name :: $detail" }
}

# ---- 0. wait for a raft king (at most 60s) ----
$king = $null
$deadline = (Get-Date).AddSeconds(60)
while ((Get-Date) -lt $deadline -and -not $king) {
    foreach ($p in $Ports) {
        $s = Http "$Base$p/api/raft/status" "GET"
        if ($s.code -eq 200 -and $s.body -match '"role":"Leader"') { $king = "$Base$p" }
    }
    if (-not $king) { Start-Sleep 3 }
}
if (-not $king) { Write-Output "FAIL no-raft-leader-in-60s"; exit 1 }
Write-Output "OK king found => $king"

# ---- 1. exactly one leader ----
$leaderCount = 0
foreach ($p in $Ports) { $s = Http "$Base$p/api/raft/status" "GET"; if ($s.body -match '"role":"Leader"') { $leaderCount++ } }
Assert "one-leader" ($leaderCount -eq 1) "count=$leaderCount"

# ---- 2. quorum write on king ----
$w = Http "$king/api/q/normal/messages" "POST" '{"content":"ci-probe"}'
Assert "king-write-201" ($w.code -eq 201) "code=$($w.code)"
$w.body -match 'ackCount":(\d+)' | Out-Null
Assert "quorum-3of3" ($Matches[1] -eq 3) "ack=$($Matches[1])"

# ---- 3. follower business write rejected ----
$others = @()
foreach ($p in $Ports) { if ("$Base$p" -ne $king) { $others += "$Base$p" } }
$rej = Http "$($others[0])/api/q/normal/messages" "POST" '{"content":"x"}'
Assert "follower-503" ($rej.code -eq 503) "code=$($rej.code)"

# ---- 4. double ack -> 404 ----
$w2 = Http "$king/api/q/error/messages" "POST" '{"content":"idem-probe"}'
$w2.body -match '"id":"([0-9a-f]+)"' | Out-Null; $id = $Matches[1]
$rr = Http "$king/api/q/error/receive"
if ($rr.body -match [regex]::Escape('"id":"' + $id + '"')) {
    Http "$king/api/q/error/ack/$id" | Out-Null
    $again = Http "$king/api/q/error/ack/$id"
    Assert "double-ack-404" ($again.code -eq 404) "code=$($again.code)"
} else { Assert "double-ack-404" $false "probe message did not come back" }

# ---- 5. DLX: topic route lands dead letter ----
Http "$king/api/ex/alerts/publish?routingKey=ops.error" "POST" '{"content":"dlx-ci"}' | Out-Null
$pl = ""
0..10 | ForEach-Object {
    $r = Http "$king/api/q/error/receive?waitMs=150"
    if ($r.code -eq 200) { $pl = $pl + $r.body }
}
Assert "dlx-not-delivered" (-not ($pl -match '"content":"dlx-ci"')) "should NOT be delivered to error queue (it is a DEAD letter)"

$dlqBody = (Http "$king/api/q/error/dlq" "GET").body
Assert "dlx-in-dlq-list" ($dlqBody -match '"content":"dlx-ci"') "dlq=$dlqBody"

# ---- 6. kill king -> new king within 70s ----
$kingPort = $king -replace [regex]::Escape($Base), ""
Write-Output "stopping king $kingPort ($($Names[$kingPort])) ..."
docker stop $Names[$kingPort] | Out-Null
$newKing = $null
$deadline = (Get-Date).AddSeconds(70)
while ((Get-Date) -lt $deadline -and -not $newKing) {
    foreach ($p in $Ports) {
        if ($p -eq $kingPort) { continue }
        $s = Http "$Base$p/api/raft/status" "GET"
        if ($s.code -eq 200 -and $s.body -match '"role":"Leader"') { $newKing = "$Base$p" }
    }
    if (-not $newKing) { Start-Sleep 4 }
}
Assert "failover-new-king" ($null -ne $newKing) "no new king within 70s"
$w3 = Http "$newKing/api/q/normal/messages" "POST" '{"content":"post-failover"}'
Assert "newking-write-201" ($w3.code -eq 201) "code=$($w3.code)"

Write-Output ""
if ($script:Fails -eq 0) { Write-Output "SMOKE OK"; exit 0 }
Write-Output "SMOKE FAIL (count=$($script:Fails))"
exit 1
