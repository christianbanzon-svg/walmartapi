param(
	[string]$ApiKey = $env:BLUECART_API_KEY,
	[string]$InputCsv = '',
	[string]$OutDir = 'walmart\output\sp_batch'
)
if (-not $ApiKey) { throw 'Set BLUECART_API_KEY or pass -ApiKey' }
if (-not $InputCsv) {
	$latest = Get-ChildItem walmart\output\walmart_scan_*.csv | Sort-Object LastWriteTime -Descending | Select-Object -First 1
	if (-not $latest) { Write-Host 'No CSV found.'; exit 1 }
	$InputCsv = $latest.FullName
}
Write-Host ("Using CSV: " + $InputCsv)
$rows = Import-Csv $InputCsv
$urls = ($rows | Where-Object { $_.seller_profile_url } | Select-Object -ExpandProperty seller_profile_url -Unique)

# ---- Helpers for polite fetching and extracting contact info ----
function Invoke-PoliteRequest([string]$url) {
	$headers = @{ "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"; "Accept-Language" = "en-US,en;q=0.9" }
	try { return Invoke-WebRequest -UseBasicParsing -Uri $url -Headers $headers -TimeoutSec 25 } catch { return $null }
}
function Extract-EmailsFromHtml([string]$html) {
	if (-not $html) { return @() }
	$set = New-Object System.Collections.Generic.HashSet[string]
	# mailto first
	foreach ($m in [regex]::Matches($html, 'mailto:([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})', 'IgnoreCase')) { [void]$set.Add($m.Groups[1].Value) }
	# plain text emails
	foreach ($m in [regex]::Matches($html, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', 'IgnoreCase')) { [void]$set.Add($m.Value) }
	return $set.ToArray()
}
function Extract-PhonesFromText([string]$text) {
	if (-not $text) { return @() }
	$set = New-Object System.Collections.Generic.HashSet[string]
	foreach ($m in [regex]::Matches($text, '(\+?\d[\d\s\-()]{7,}\d)', 'IgnoreCase')) { [void]$set.Add(($m.Value -replace '\s+', ' ').Trim()) }
	return $set.ToArray()
}
function ScrapeEmailPhoneFromSellerPage([string]$profileUrl) {
	$result = @{ email = $null; phone = $null }
	if (-not $profileUrl) { return $result }
	$page = Invoke-PoliteRequest $profileUrl
	if ($page -and $page.Content) {
		$emails = Extract-EmailsFromHtml $page.Content
		$phones = Extract-PhonesFromText $page.Content
		if ($emails.Count -gt 0) { $result.email = $emails[0] }
		if ($phones.Count -gt 0) { $result.phone = $phones[0] }
		# follow likely contact/policy links if still empty
		if (-not $result.email -or -not $result.phone) {
			foreach ($a in ([regex]::Matches($page.Content, '<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', 'Singleline,IgnoreCase'))) {
				$href = $a.Groups[1].Value; $txt = ($a.Groups[2].Value -replace '<[^>]+>', '').ToLower()
				if ($txt -match 'contact|support|about|policy|return|help') {
					try {
						$link = if ($href -like 'http*') { $href } else { (New-Object System.Uri((New-Object System.Uri($profileUrl)), $href)).AbsoluteUri }
						$p2 = Invoke-PoliteRequest $link
						if ($p2 -and $p2.Content) {
							if (-not $result.email) { $e2 = Extract-EmailsFromHtml $p2.Content; if ($e2.Count -gt 0) { $result.email = $e2[0] } }
							if (-not $result.phone) { $ph2 = Extract-PhonesFromText $p2.Content; if ($ph2.Count -gt 0) { $result.phone = $ph2[0] } }
						}
					} catch {}
				}
			}
		}
	}
	return $result
}

function Get-SellerIdFromUrl([string]$u) {
	if ($u -match '/seller/(\d+)') { return $matches[1] }
	if ($u -match '(^|[?&])selectedSellerId=(\d+)') { return $matches[2] }
	if ($u -match '(^|[?&])sellerId=(\d+)') { return $matches[2] }
	return $null
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$summary = @()
$i = 0
foreach ($u in $urls) {
	$i++
	$sid = Get-SellerIdFromUrl $u
	$args = @('-sS','--ssl-no-revoke','-G','https://api.bluecartapi.com/request','--data-urlencode',("api_key="+$ApiKey),'--data-urlencode','type=seller_profile')
	if ($sid) { $args += @('--data-urlencode',("seller_id="+$sid)) } else { $args += @('--data-urlencode',("url="+$u)) }
	$resp = & curl.exe @args
	$safe = ($u -replace '[^A-Za-z0-9]','_')
	Set-Content (Join-Path $OutDir ("sp_url_"+$safe+".json")) -Value $resp -Encoding UTF8
	try {
		$j = $resp | ConvertFrom-Json
		$seller = $j.seller_details; if (-not $seller) { $seller = $j.seller }
		$email = $null; $phone = $null; $name = $null; $rating = $null; $ratings_total = $null; $percent_positive = $null
		if ($seller) {
			$name = $seller.name; $phone = $seller.phone; $email = $seller.email; $rating = $seller.rating; $ratings_total = $seller.ratings_total; $percent_positive = $seller.percent_positive
		}
		# Scrape fallback if missing email or phone
		if (-not $email -or -not $phone) {
			$sf = ScrapeEmailPhoneFromSellerPage $u
			if (-not $email -and $sf.email) { $email = $sf.email }
			if (-not $phone -and $sf.phone) { $phone = $sf.phone }
		}
		if ($name -or $email -or $phone -or $rating) {
			$summary += [pscustomobject]@{ url=$u; seller_id=$sid; name=$name; phone=$phone; email=$email; rating=$rating; ratings_total=$ratings_total; percent_positive=$percent_positive; message='' }
		} else {
			$msg = ''; if ($j.request_info) { $msg = $j.request_info.message }
			$summary += [pscustomobject]@{ url=$u; seller_id=$sid; name=''; phone=$phone; email=$email; rating=''; ratings_total=''; percent_positive=''; message=$msg }
		}
	} catch {
		$summary += [pscustomobject]@{ url=$u; seller_id=$sid; message=('PARSE_ERROR: ' + $_.Exception.Message) }
	}
	Start-Sleep -Milliseconds 150
}
$summaryPath = Join-Path $OutDir 'seller_profile_summary.csv'
$summary | Export-Csv -Path $summaryPath -NoTypeInformation -Encoding UTF8
Write-Host ("Summary written: " + $summaryPath)
