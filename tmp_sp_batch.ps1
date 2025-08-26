$ErrorActionPreference='Stop'
cd "E:\prog fold\walmart"

$k = $env:BLUECART_API_KEY
if (-not $k) { throw "Set BLUECART_API_KEY in your environment." }

$latest = Get-ChildItem walmart\output\walmart_scan_*.csv | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $latest) { Write-Host 'No CSV found.'; exit 1 }
$rows = Import-Csv $latest.FullName
$urls = ($rows | Where-Object { $_.seller_profile_url } | Select-Object -ExpandProperty seller_profile_url -Unique)

function Get-SellerIdFromUrl([string]$u) {
  if ($u -match '/seller/(\d+)') { return $matches[1] }
  if ($u -match '(^|[?&])selectedSellerId=(\d+)') { return $matches[2] }
  if ($u -match '(^|[?&])sellerId=(\d+)') { return $matches[2] }
  return $null
}

$outDir = 'walmart\output\sp_batch'
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$summary = @()

$i=0
foreach ($u in $urls) {
  $i++
  $sellerId = Get-SellerIdFromUrl $u
  $args = @('-sS','--ssl-no-revoke','-G','https://api.bluecartapi.com/request',
            '--data-urlencode',("api_key="+$k),'--data-urlencode','type=seller_profile')
  if ($sellerId) { $args += @('--data-urlencode',("seller_id="+$sellerId)) } else { $args += @('--data-urlencode',("url="+$u)) }

  $resp = & curl.exe @args
  $safe = ($u -replace '[^A-Za-z0-9]','_')
  Set-Content (Join-Path $outDir "sp_url_$safe.json") -Value $resp -Encoding UTF8

  try {
    $j = $resp | ConvertFrom-Json
    $seller = $j.seller_details
    if (-not $seller) { $seller = $j.seller }
    if ($seller) {
      $summary += [pscustomobject]@{ url=$u; seller_id=$sellerId; name=$seller.name; phone=$seller.phone; email=$seller.email; rating=$seller.rating; ratings_total=$seller.ratings_total; percent_positive=$seller.percent_positive; message='' }
    } else {
      $msg = ''; if ($j.request_info) { $msg = $j.request_info.message }
      $summary += [pscustomobject]@{ url=$u; seller_id=$sellerId; name=''; phone=''; email=''; rating=''; ratings_total=''; percent_positive=''; message=$msg }
    }
  } catch {
    $summary += [pscustomobject]@{ url=$u; seller_id=$sellerId; message=('PARSE_ERROR: ' + $_.Exception.Message) }
  }
  Start-Sleep -Milliseconds 150
}

$summary | Export-Csv (Join-Path $outDir 'seller_profile_summary.csv') -NoTypeInformation -Encoding UTF8
Write-Host 'Done.'
