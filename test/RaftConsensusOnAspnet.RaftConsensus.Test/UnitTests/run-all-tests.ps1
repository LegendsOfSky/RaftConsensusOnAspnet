# Run every test.py under this directory with python3 and print a pass/fail summary.
# Tests run one at a time because each one starts Docker and binds the same host port.
# A test passes when python3 exits 0. test.py exits 1 when the check fails.

$ErrorActionPreference = "Continue"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$root = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($root)) {
    $root = (Get-Location).Path
}

if (-not (Get-Command python3 -ErrorAction SilentlyContinue)) {
    Write-Host "python3 was not found on PATH."
    exit 127
}

$tests = @(Get-ChildItem -Path $root -Recurse -File -Filter "test.py" | Sort-Object -Property FullName)
if ($tests.Count -eq 0) {
    Write-Host "No test.py files found under $root"
    exit 1
}

Write-Host "Found $($tests.Count) test.py file(s). Running them one at a time."

$passed = [System.Collections.Generic.List[string]]::new()
$failed = [System.Collections.Generic.List[string]]::new()
$index = 0

foreach ($test in $tests) {
    $index++
    $relative = $test.DirectoryName
    $prefix = $root.TrimEnd('\', '/')
    if ($relative.Equals($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        $relative = "."
    }
    elseif ($relative.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        $relative = $relative.Substring($prefix.Length).TrimStart('\', '/')
    }
    $relative = ($relative -replace '\\', '/')

    Write-Host ""
    Write-Host "========== [$index/$($tests.Count)] $relative =========="

    $exitCode = 1
    Push-Location $test.DirectoryName
    try {
        # cmd merges both streams and the pipeline waits until they are printed.
        # A direct python3 call can leave stderr showing up under the next test.
        & cmd.exe /c "python3 -u test.py 2>&1" | ForEach-Object {
            Write-Host ("$_".TrimEnd("`r"))
        }
        if ($null -eq $LASTEXITCODE) {
            $exitCode = 1
        }
        else {
            $exitCode = $LASTEXITCODE
        }
    }
    catch {
        Write-Host $_.Exception.Message
        $exitCode = 1
    }
    finally {
        Pop-Location
    }

    if ($exitCode -eq 0) {
        [void]$passed.Add($relative)
        Write-Host "Result: PASSED"
    }
    else {
        [void]$failed.Add("$relative (exit $exitCode)")
        Write-Host "Result: FAILED (exit $exitCode)"
    }
}

Write-Host ""
Write-Host "========== Summary =========="
Write-Host "Passed ($($passed.Count)):"
if ($passed.Count -eq 0) {
    Write-Host "  (none)"
}
else {
    foreach ($item in $passed) {
        Write-Host "  $item"
    }
}

Write-Host "Failed ($($failed.Count)):"
if ($failed.Count -eq 0) {
    Write-Host "  (none)"
}
else {
    foreach ($item in $failed) {
        Write-Host "  $item"
    }
}

Write-Host "Total: $($tests.Count)  Passed: $($passed.Count)  Failed: $($failed.Count)"

if ($failed.Count -gt 0) {
    exit 1
}

exit 0
