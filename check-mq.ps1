# Unified message probe (ASCII-only script)
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
echo "=== SEND ==="
$body = [System.Text.Encoding]::UTF8.GetBytes('{"content":"unified-msg-check","key":"route-66"}')
$r = Invoke-RestMethod -Method Post http://localhost:5080/api/q/normal/messages -ContentType "application/json" -Body $body
$r | ConvertTo-Json -Compress
echo "=== RECEIVE (ReceiveTime should be filled) ==="
$m = Invoke-RestMethod -Method Post "http://localhost:5080/api/q/normal/receive?visibilitySeconds=60"
$m | ConvertTo-Json -Compress



